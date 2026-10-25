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
	"strings"
	"testing"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/internal/wireprobe"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func archiveOverGRPC(t *testing.T, settings map[string]string) (*objstorepb.ArchiveResponse, error) {
	t.Helper()
	storage := newMockLifecycleStorage()
	storage.data["test-key"] = []byte("data")
	storage.metadata["test-key"] = &common.Metadata{Size: 4}
	server, err := newTestServer(t, storage)
	if err != nil {
		t.Fatalf("create server: %v", err)
	}
	return server.Archive(context.Background(), &objstorepb.ArchiveRequest{
		Key: "test-key", DestinationType: wireprobe.Backend, DestinationSettings: settings,
	})
}

func TestArchiveCredentialReachesBackendThroughHandle(t *testing.T) {
	probe := wireprobe.Register(t)
	req := map[string]string{"accessKey": "AK-grpc", "secretKey": "SK-grpc", "region": "eu-1"}
	resp, err := archiveOverGRPC(t, req)
	if err != nil || !resp.Success {
		t.Fatalf("Archive: resp=%v err=%v", resp, err)
	}
	probe.AssertCredential(t, "AK-grpc", "SK-grpc", map[string]string{"region": "eu-1"})
}

func TestArchiveRefusesTokenSettings(t *testing.T) {
	probe := wireprobe.Register(t)
	_, err := archiveOverGRPC(t, map[string]string{"sessionToken": "tok-grpc"})
	if err == nil {
		t.Fatal("a token left in settings must be refused")
	}
	if probe.Calls() != 0 || strings.Contains(err.Error(), "tok-grpc") {
		t.Errorf("configured %d times, or the error echoes the token", probe.Calls())
	}
}
