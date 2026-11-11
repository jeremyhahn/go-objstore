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
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

// allowLocalArchive is the wire option of a server that allows client-supplied
// local archive destinations; tests that archive to local on purpose use it.
var allowLocalArchive = factory.WireOptions{AllowLocalDestinations: true}

func newLocalArchiveTestServer(t *testing.T, opts ...ServerOption) *Server {
	t.Helper()
	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}, common.Credentials{}); err != nil {
		t.Fatalf("configure storage: %v", err)
	}
	if err := storage.Put("obj.txt", bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("put: %v", err)
	}
	srv, err := newTestServer(t, storage, opts...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return srv
}

func TestArchiveRefusesClientLocalDestinationByDefault(t *testing.T) {
	srv := newLocalArchiveTestServer(t)
	dest := filepath.Join(t.TempDir(), "outside")

	_, err := srv.Archive(context.Background(), &objstorepb.ArchiveRequest{
		Key:                 "obj.txt",
		DestinationType:     "local",
		DestinationSettings: map[string]string{"path": dest},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Archive: code %v (%v), want InvalidArgument", status.Code(err), err)
	}
	if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
		t.Fatalf("refused archive must not create %s (stat: %v)", dest, statErr)
	}

	_, err = srv.AddPolicy(context.Background(), &objstorepb.AddPolicyRequest{Policy: &objstorepb.LifecyclePolicy{
		Id:                  "p1",
		Action:              "archive",
		RetentionSeconds:    60,
		DestinationType:     "local",
		DestinationSettings: map[string]string{"path": dest},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AddPolicy: code %v (%v), want InvalidArgument", status.Code(err), err)
	}
}

func TestArchiveAllowsClientLocalDestinationWhenSet(t *testing.T) {
	srv := newLocalArchiveTestServer(t, WithAllowClientLocalArchiveDestinations(true))
	dest := t.TempDir()

	if _, err := srv.Archive(context.Background(), &objstorepb.ArchiveRequest{
		Key:                 "obj.txt",
		DestinationType:     "local",
		DestinationSettings: map[string]string{"path": dest},
	}); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dest, "obj.txt")); err != nil {
		t.Fatalf("archived object: %v", err)
	}
}
