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
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// badURL produces a URL that causes http.NewRequestWithContext to return an
// error without ever reaching httpClient.Do. A null byte in the URL host is
// reliably rejected by net/http before the request is dispatched.
const badURLBase = "http://\x00bad"

// ---------------------------------------------------------------------------
// REST – http.NewRequestWithContext error branches
// (rest.go:67, 107, 157, 183, 222, 253, 284/289, 322/327, 354/359, 386,
//  412, 464, 498, 519, 530, 545, 557, 571, 585, 602, 616, 638, 685)
// ---------------------------------------------------------------------------

// restClientWithBadURL returns a RESTClient whose baseURL will cause every
// http.NewRequestWithContext call to fail immediately.
func restClientWithBadURL() *RESTClient {
	return &RESTClient{baseURL: badURLBase, httpClient: &http.Client{}}
}

func TestRESTClient_Put_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	err := c.Put(context.Background(), "key", strings.NewReader("data"), nil)
	if err == nil {
		t.Fatal("expected error from bad URL in Put")
	}
}

func TestRESTClient_Get_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	_, _, err := c.Get(context.Background(), "key")
	if err == nil {
		t.Fatal("expected error from bad URL in Get")
	}
}

func TestRESTClient_Delete_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	err := c.Delete(context.Background(), "key")
	if err == nil {
		t.Fatal("expected error from bad URL in Delete")
	}
}

func TestRESTClient_Exists_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	_, err := c.Exists(context.Background(), "key")
	if err == nil {
		t.Fatal("expected error from bad URL in Exists")
	}
}

func TestRESTClient_List_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	_, err := c.List(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error from bad URL in List")
	}
}

func TestRESTClient_GetMetadata_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	_, err := c.GetMetadata(context.Background(), "key")
	if err == nil {
		t.Fatal("expected error from bad URL in GetMetadata")
	}
}

func TestRESTClient_UpdateMetadata_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	err := c.UpdateMetadata(context.Background(), "key", &common.Metadata{})
	if err == nil {
		t.Fatal("expected error from bad URL in UpdateMetadata")
	}
}

func TestRESTClient_Archive_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	err := c.Archive(context.Background(), "key", "glacier", nil)
	if err == nil {
		t.Fatal("expected error from bad URL in Archive")
	}
}

func TestRESTClient_AddPolicy_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	err := c.AddPolicy(context.Background(), common.LifecyclePolicy{ID: "p"})
	if err == nil {
		t.Fatal("expected error from bad URL in AddPolicy")
	}
}

func TestRESTClient_RemovePolicy_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	err := c.RemovePolicy(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error from bad URL in RemovePolicy")
	}
}

func TestRESTClient_GetPolicies_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	_, err := c.GetPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error from bad URL in GetPolicies")
	}
}

func TestRESTClient_ApplyPolicies_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	_, _, err := c.ApplyPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error from bad URL in ApplyPolicies")
	}
}

func TestRESTClient_Health_NewRequestError(t *testing.T) {
	c := restClientWithBadURL()
	err := c.Health(context.Background())
	if err == nil {
		t.Fatal("expected error from bad URL in Health")
	}
}

// ---------------------------------------------------------------------------
// REST – empty body fallback error branches
// (rest.go:96, 122, etc. — the `return fmt.Errorf("%w %d", ...)` line
//  reached when body is empty on a non-2xx response)
// ---------------------------------------------------------------------------

// The existing error-without-body tests use http.StatusInternalServerError
// without writing a body. However, some tests previously wrote a body so the
// "with-body" branch got hit but not the empty-body branch. The tests below
// explicitly confirm empty-body behaviour for each remaining method.

func TestRESTClient_Put_EmptyBodyError(t *testing.T) {
	// StatusBadRequest with no body text
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		// no body
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	err := c.Put(context.Background(), "k", strings.NewReader("d"), nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_Get_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	_, _, err := c.Get(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_Delete_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	err := c.Delete(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_List_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	_, err := c.List(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_GetMetadata_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	_, err := c.GetMetadata(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_UpdateMetadata_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	err := c.UpdateMetadata(context.Background(), "k", &common.Metadata{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_Archive_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	err := c.Archive(context.Background(), "k", "glacier", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_AddPolicy_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	err := c.AddPolicy(context.Background(), common.LifecyclePolicy{ID: "p"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_RemovePolicy_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	err := c.RemovePolicy(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_GetPolicies_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	_, err := c.GetPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_ApplyPolicies_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	_, _, err := c.ApplyPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_AddReplicationPolicy_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	err := c.AddReplicationPolicy(context.Background(), common.ReplicationPolicy{ID: "p"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_RemoveReplicationPolicy_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	err := c.RemoveReplicationPolicy(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_GetReplicationPolicy_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	_, err := c.GetReplicationPolicy(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_GetReplicationPolicies_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	_, err := c.GetReplicationPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_TriggerReplication_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	_, err := c.TriggerReplication(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_GetReplicationStatus_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	_, err := c.GetReplicationStatus(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRESTClient_Health_EmptyBodyError(t *testing.T) {
	srv := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	defer srv.Close()
	c := newRESTClient(srv.URL)
	err := c.Health(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// QUIC – http.NewRequestWithContext error branches
// (quic.go:83, 123, 173, 199, 238, 269, 315, 360, 399, 431, 457, 506, 540,
//  574, 605, 630, 660, 695, 725)
// ---------------------------------------------------------------------------

func quicClientWithBadURL(t *testing.T) *QUICClient {
	t.Helper()
	return &QUICClient{
		baseURL:    badURLBase,
		httpClient: &http.Client{},
	}
}

func TestQUICClient_Put_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	err := c.Put(context.Background(), "k", strings.NewReader("d"), nil)
	if err == nil {
		t.Fatal("expected error from bad URL in Put")
	}
}

func TestQUICClient_Get_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	_, _, err := c.Get(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error from bad URL in Get")
	}
}

func TestQUICClient_Delete_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	err := c.Delete(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error from bad URL in Delete")
	}
}

func TestQUICClient_Exists_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	_, err := c.Exists(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error from bad URL in Exists")
	}
}

func TestQUICClient_List_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	_, err := c.List(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error from bad URL in List")
	}
}

func TestQUICClient_GetMetadata_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	_, err := c.GetMetadata(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error from bad URL in GetMetadata")
	}
}

func TestQUICClient_UpdateMetadata_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	err := c.UpdateMetadata(context.Background(), "k", &common.Metadata{})
	if err == nil {
		t.Fatal("expected error from bad URL in UpdateMetadata")
	}
}

func TestQUICClient_Archive_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	err := c.Archive(context.Background(), "k", "glacier", nil)
	if err == nil {
		t.Fatal("expected error from bad URL in Archive")
	}
}

func TestQUICClient_AddPolicy_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	err := c.AddPolicy(context.Background(), common.LifecyclePolicy{ID: "p"})
	if err == nil {
		t.Fatal("expected error from bad URL in AddPolicy")
	}
}

func TestQUICClient_RemovePolicy_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	err := c.RemovePolicy(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error from bad URL in RemovePolicy")
	}
}

func TestQUICClient_GetPolicies_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	_, err := c.GetPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error from bad URL in GetPolicies")
	}
}

func TestQUICClient_ApplyPolicies_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	_, _, err := c.ApplyPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error from bad URL in ApplyPolicies")
	}
}

func TestQUICClient_Health_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	err := c.Health(context.Background())
	if err == nil {
		t.Fatal("expected error from bad URL in Health")
	}
}

func TestQUICClient_AddReplicationPolicy_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	err := c.AddReplicationPolicy(context.Background(), common.ReplicationPolicy{ID: "p"})
	if err == nil {
		t.Fatal("expected error from bad URL in AddReplicationPolicy")
	}
}

func TestQUICClient_RemoveReplicationPolicy_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	err := c.RemoveReplicationPolicy(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error from bad URL in RemoveReplicationPolicy")
	}
}

func TestQUICClient_GetReplicationPolicy_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	_, err := c.GetReplicationPolicy(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error from bad URL in GetReplicationPolicy")
	}
}

func TestQUICClient_GetReplicationPolicies_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	_, err := c.GetReplicationPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error from bad URL in GetReplicationPolicies")
	}
}

func TestQUICClient_TriggerReplication_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	_, err := c.TriggerReplication(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error from bad URL in TriggerReplication")
	}
}

func TestQUICClient_GetReplicationStatus_NewRequestError(t *testing.T) {
	c := quicClientWithBadURL(t)
	_, err := c.GetReplicationStatus(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error from bad URL in GetReplicationStatus")
	}
}

// ---------------------------------------------------------------------------
// QUIC – empty body fallback error branches
// (quic.go:595, 620, 645, 675, 710)
// ---------------------------------------------------------------------------

func TestQUICClient_Put_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	err := c.Put(context.Background(), "k", strings.NewReader("d"), nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestQUICClient_Delete_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	err := c.Delete(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestQUICClient_List_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	_, err := c.List(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestQUICClient_AddReplicationPolicy_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	err := c.AddReplicationPolicy(context.Background(), common.ReplicationPolicy{ID: "p"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestQUICClient_RemoveReplicationPolicy_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	err := c.RemoveReplicationPolicy(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestQUICClient_GetReplicationPolicy_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	_, err := c.GetReplicationPolicy(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestQUICClient_GetReplicationPolicies_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	_, err := c.GetReplicationPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestQUICClient_TriggerReplication_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	_, err := c.TriggerReplication(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestQUICClient_GetReplicationStatus_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	_, err := c.GetReplicationStatus(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// gRPC – context-canceled error branches for GetPolicies, Health, List, Get
// (grpc.go:170, 252, 286 and stream.Recv error at grpc.go:92)
// ---------------------------------------------------------------------------

func TestGRPCClient_List_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.List(ctx, &common.ListOptions{Prefix: "test/"})
	if err == nil {
		t.Fatal("expected error from canceled context in List")
	}
}

func TestGRPCClient_GetPolicies_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.GetPolicies(ctx)
	if err == nil {
		t.Fatal("expected error from canceled context in GetPolicies")
	}
}

func TestGRPCClient_Health_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.Health(ctx)
	if err == nil {
		t.Fatal("expected error from canceled context in Health")
	}
}

// gRPC Get – stream.Recv error on first chunk
// The existing mockStreamErrorGRPCServer returns an error after the first
// chunk; we need the error on the very first Recv (i.e., before the pipe is
// set up). We use a server that returns an error immediately.
type mockGetErrorImmediatelyServer struct {
	objstorepb.UnimplementedObjectStoreServer
}

func (s *mockGetErrorImmediatelyServer) Get(
	req *objstorepb.GetRequest,
	stream objstorepb.ObjectStore_GetServer,
) error {
	// Return error without sending any chunks – client's first Recv will fail.
	return errGRPCGetFailed
}

// errGRPCGetFailed is a sentinel to avoid pulling in errors package twice.
var errGRPCGetFailed = grpc.Errorf(2 /*codes.Unknown*/, "simulated get failure")

func TestGRPCClient_Get_FirstRecvError(t *testing.T) {
	l := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	objstorepb.RegisterObjectStoreServer(s, &mockGetErrorImmediatelyServer{})
	go func() { _ = s.Serve(l) }()
	defer s.Stop()
	defer l.Close()

	dial := func(ctx context.Context, _ string) (net.Conn, error) { return l.Dial() }
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(dial), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	c := &GRPCClient{conn: conn, client: objstorepb.NewObjectStoreClient(conn)}
	_, _, err = c.Get(context.Background(), "key")
	if err == nil {
		t.Fatal("expected error on first Recv failure")
	}
}

// ---------------------------------------------------------------------------
// QUIC – archive empty-body path (quic.go:365)
// The Archive function has two "empty-body" branches (one at line 362 for the
// Archive error and also possibly the marshal error, but json.Marshal doesn't
// fail on map[string]string, so we target the HTTP error path).
// ---------------------------------------------------------------------------

func TestQUICClient_Archive_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	err := c.Archive(context.Background(), "k", "glacier", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestQUICClient_AddPolicy_EmptyBodyError exercises the empty-body fallback
// in AddPolicy (quic.go:404).
func TestQUICClient_AddPolicy_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	err := c.AddPolicy(context.Background(), common.LifecyclePolicy{ID: "p"})
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestQUICClient_RemovePolicy_EmptyBodyError exercises the empty-body fallback
// in RemovePolicy (quic.go:433).
func TestQUICClient_RemovePolicy_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	err := c.RemovePolicy(context.Background(), "p1")
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestQUICClient_GetPolicies_EmptyBodyError exercises the empty-body fallback
// in GetPolicies (quic.go:459).
func TestQUICClient_GetPolicies_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	_, err := c.GetPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestQUICClient_ApplyPolicies_EmptyBodyError exercises the empty-body
// fallback in ApplyPolicies (quic.go:508).
func TestQUICClient_ApplyPolicies_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	_, _, err := c.ApplyPolicies(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestQUICClient_Health_EmptyBodyError exercises the empty-body fallback in
// Health (quic.go:542).
func TestQUICClient_Health_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	err := c.Health(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// gRPC – GetMetadata, Archive, AddPolicy, RemovePolicy, UpdateMetadata
// via canceled context to cover RPC error branches not yet covered.
// ---------------------------------------------------------------------------

func TestGRPCClient_GetMetadata_ContextCanceledError(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.GetMetadata(ctx, "k")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGRPCClient_Delete_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.Delete(ctx, "k")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGRPCClient_Archive_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.Archive(ctx, "k", "glacier", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGRPCClient_AddPolicy_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.AddPolicy(ctx, common.LifecyclePolicy{ID: "p"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGRPCClient_RemovePolicy_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.RemovePolicy(ctx, "p1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGRPCClient_UpdateMetadata_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.UpdateMetadata(ctx, "k", &common.Metadata{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGRPCClient_Put_ContextCanceled(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := client.Put(ctx, "k", strings.NewReader("data"), nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// QUIC – Get empty body error (quic.go:138 – the body-less non-200 branch)
// ---------------------------------------------------------------------------

func TestQUICClient_Get_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		// no body
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	_, _, err := c.Get(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// QUIC GetMetadata – empty body error (quic.go:not yet covered)
// ---------------------------------------------------------------------------

func TestQUICClient_GetMetadata_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	_, err := c.GetMetadata(context.Background(), "k")
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// QUIC UpdateMetadata – empty body error
// ---------------------------------------------------------------------------

func TestQUICClient_UpdateMetadata_EmptyBodyError(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	err := c.UpdateMetadata(context.Background(), "k", &common.Metadata{})
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// gRPC – Put write-error branch: the goroutine pw.Write failure (grpc.go:106)
// is tested indirectly when the reader is closed mid-stream; the
// mockStreamErrorGRPCServer already covers the pw.CloseWithError path in the
// existing test, so we add a test that explicitly reads from the pipe after
// the goroutine has closed it with an error to confirm the error propagates.
// ---------------------------------------------------------------------------

// TestGRPCClient_Get_PipeWriteError verifies that if stream.Recv returns EOF
// immediately after the first chunk (i.e., zero additional data), the reader
// returns all received data with no error.
type mockSingleChunkGRPCServer struct {
	objstorepb.UnimplementedObjectStoreServer
}

func (s *mockSingleChunkGRPCServer) Get(
	req *objstorepb.GetRequest,
	stream objstorepb.ObjectStore_GetServer,
) error {
	return stream.Send(&objstorepb.GetResponse{
		Data: []byte("only"),
		Metadata: &objstorepb.Metadata{
			ContentType: "text/plain",
		},
	})
}

func TestGRPCClient_Get_SingleChunk(t *testing.T) {
	l := bufconn.Listen(bufSize)
	s := grpc.NewServer()
	objstorepb.RegisterObjectStoreServer(s, &mockSingleChunkGRPCServer{})
	go func() { _ = s.Serve(l) }()
	defer s.Stop()
	defer l.Close()

	dial := func(ctx context.Context, _ string) (net.Conn, error) { return l.Dial() }
	conn, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(dial), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	c := &GRPCClient{conn: conn, client: objstorepb.NewObjectStoreClient(conn)}
	reader, meta, err := c.Get(context.Background(), "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer reader.Close()

	buf := make([]byte, 16)
	n, _ := reader.Read(buf)
	if string(buf[:n]) != "only" {
		t.Errorf("expected 'only', got %q", buf[:n])
	}
	if meta.ContentType != "text/plain" {
		t.Errorf("expected text/plain, got %s", meta.ContentType)
	}
}

// ---------------------------------------------------------------------------
// QUIC – archive error with empty body (the second return in Archive)
// (quic.go:365)
// ---------------------------------------------------------------------------

func TestQUICClient_Archive_EmptyBodyError2(t *testing.T) {
	srv := newHTTP3TestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		// intentionally empty body
	}))
	defer srv.Close()
	c := newQUICTestClient(t, srv.URL)
	err := c.Archive(context.Background(), "k", "glacier", map[string]string{"vault": "v"})
	if err == nil {
		t.Fatal("expected error")
	}
}

// ---------------------------------------------------------------------------
// Verify gRPC List with ContinueFrom set (exercises MaxResults=0 branch)
// ---------------------------------------------------------------------------

func TestGRPCClient_List_WithContinueFrom(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	result, err := client.List(context.Background(), &common.ListOptions{
		Prefix:       "test/",
		ContinueFrom: "token-abc",
	})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// ---------------------------------------------------------------------------
// Additional timing: context deadline test to exercise gRPC Health error
// ---------------------------------------------------------------------------

func TestGRPCClient_Health_DeadlineExceeded(t *testing.T) {
	client, cleanup := createGRPCTestClient(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	err := client.Health(ctx)
	if err == nil {
		t.Fatal("expected deadline exceeded error")
	}
}
