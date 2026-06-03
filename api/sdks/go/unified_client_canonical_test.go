// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

// This file implements the canonical unified-client cases: NewClient must
// construct the right protocol client for each protocol (delegation), reject
// unknown protocols, and Close must release resources without error.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestUnifiedClientCanonical(t *testing.T) {
	ctx := context.Background()

	t.Run("unified_delegates_rest", func(t *testing.T) {
		// NewClient(REST) yields a *RESTClient that delegates a real call.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"status": "serving", "version": "1.0"})
		}))
		defer srv.Close()

		client, err := NewClient(&ClientConfig{Protocol: ProtocolREST, Address: srv.Listener.Addr().String()})
		require.NoError(t, err)
		assert.IsType(t, &RESTClient{}, client)

		st, err := client.Health(ctx)
		require.NoError(t, err)
		assert.Equal(t, "SERVING", st.Status)
	})

	t.Run("unified_delegates_grpc", func(t *testing.T) {
		// NewClient(gRPC) yields a *GRPCClient; we swap in a mocked stub to
		// verify the unified client delegates a representative call.
		client, err := NewClient(&ClientConfig{Protocol: ProtocolGRPC, Address: "localhost:50051"})
		require.NoError(t, err)
		gc, ok := client.(*GRPCClient)
		require.True(t, ok)
		defer gc.Close()

		stub, mc := newGRPCMock()
		gc.client = stub.client
		mc.On("Put", ctx, mock.Anything).Return(&objstorepb.PutResponse{Success: true}, nil)

		res, err := gc.Put(ctx, "k", []byte("d"), nil)
		require.NoError(t, err)
		assert.True(t, res.Success)
	})

	t.Run("unified_delegates_quic", func(t *testing.T) {
		// NewClient(QUIC) yields a *QUICClient that delegates a real call once
		// pointed at a test server.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"status": "serving", "protocol": "quic"})
		}))
		defer srv.Close()

		client, err := NewClient(&ClientConfig{
			Protocol:           ProtocolQUIC,
			Address:            srv.Listener.Addr().String(),
			UseTLS:             true,
			InsecureSkipVerify: true,
		})
		require.NoError(t, err)
		qc, ok := client.(*QUICClient)
		require.True(t, ok)
		qc.httpClient = srv.Client()
		qc.baseURL = srv.URL

		st, err := qc.Health(ctx)
		require.NoError(t, err)
		assert.Equal(t, "SERVING", st.Status)
	})

	t.Run("unified_invalid_protocol", func(t *testing.T) {
		_, err := NewClient(&ClientConfig{Protocol: "carrier-pigeon", Address: "x"})
		assert.ErrorIs(t, err, ErrInvalidProtocol)
	})

	t.Run("unified_close_rest", func(t *testing.T) {
		client, err := NewClient(&ClientConfig{Protocol: ProtocolREST, Address: "localhost:8080"})
		require.NoError(t, err)
		assert.NoError(t, client.Close())
		// Close must be safe to call again.
		assert.NoError(t, client.Close())
	})

	t.Run("unified_close_grpc", func(t *testing.T) {
		client, err := NewClient(&ClientConfig{Protocol: ProtocolGRPC, Address: "localhost:50051"})
		require.NoError(t, err)
		assert.NoError(t, client.Close())
	})

	t.Run("unified_close_quic", func(t *testing.T) {
		client, err := NewClient(&ClientConfig{
			Protocol:           ProtocolQUIC,
			Address:            "localhost:4433",
			UseTLS:             true,
			InsecureSkipVerify: true,
		})
		require.NoError(t, err)
		assert.NoError(t, client.Close())
	})
}
