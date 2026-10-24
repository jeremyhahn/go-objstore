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

package factory

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/memory"
)

func TestNewBucketManager(t *testing.T) {
	root := filepath.Join(t.TempDir(), "buckets")

	tests := []struct {
		name     string
		backend  string
		settings map[string]string
		wantErr  error
	}{
		{"memory", "memory", nil, nil},
		{"local with root", "local", map[string]string{"bucketRoot": root}, nil},
		{"local without root", "local", map[string]string{"path": t.TempDir()}, common.ErrBucketRootNotSet},
		{"local bad expiry", "local", map[string]string{"bucketRoot": root, "multipartExpiry": "x"}, common.ErrInvalidArgument},
		{"glacier", "glacier", nil, ErrArchiveOnlyBackend},
		{"azurearchive", "azurearchive", nil, ErrArchiveOnlyBackend},
		{"unknown", "no-such-backend", nil, ErrUnknownBackend},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bm, err := NewBucketManager(tt.backend, tt.settings)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				if bm != nil {
					t.Errorf("manager = %v with an error", bm)
				}
				return
			}
			if err != nil || bm == nil {
				t.Fatalf("NewBucketManager = %v, %v", bm, err)
			}
			ctx := context.Background()
			if err := bm.CreateBucket(ctx, "bucket-one"); err != nil {
				t.Fatal(err)
			}
			infos, err := bm.ListBuckets(ctx)
			if err != nil || len(infos) != 1 || infos[0].Name != "bucket-one" {
				t.Errorf("ListBuckets = %+v, %v", infos, err)
			}
		})
	}
}

func TestNewBucketManager_RegisteredStorageWithoutManager(t *testing.T) {
	const name = "buckets-test-no-manager"
	RegisterStorage(name, func(map[string]string) (common.Storage, error) { return memory.New(), nil })
	t.Cleanup(func() { delete(storageRegistry, name) })

	bm, err := NewBucketManager(name, nil)
	if !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("err = %v, want ErrNotSupported", err)
	}
	if bm != nil {
		t.Errorf("manager = %v with an error", bm)
	}
}

func TestRegisterBucketManager(t *testing.T) {
	const name = "buckets-test-custom"
	want := errors.New("creator failed")
	var got map[string]string
	RegisterBucketManager(name, func(settings map[string]string) (common.BucketManager, error) {
		got = settings
		return nil, want
	})
	t.Cleanup(func() { delete(bucketManagerRegistry, name) })

	_, err := NewBucketManager(name, map[string]string{"a": "b"})
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want the creator's error", err)
	}
	if got["a"] != "b" {
		t.Errorf("settings not passed through: %v", got)
	}
}

func TestNewBucketManager_MemoryManagersAreIndependent(t *testing.T) {
	ctx := context.Background()
	a, _ := NewBucketManager("memory", nil)
	b, _ := NewBucketManager("memory", nil)
	if err := a.CreateBucket(ctx, "only-in-a"); err != nil {
		t.Fatal(err)
	}
	if infos, _ := b.ListBuckets(ctx); len(infos) != 0 {
		t.Errorf("a bucket leaked between managers: %v", infos)
	}
}
