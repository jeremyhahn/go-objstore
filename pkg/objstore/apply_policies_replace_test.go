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

package objstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

// replacingLister replaces one object (a successful PUT) when the second
// listing page is requested, after ApplyPolicies queued its first page.
type replacingLister struct {
	*local.Local
	t     *testing.T
	key   string
	calls int
}

func (r *replacingLister) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	r.calls++
	if r.calls == 2 {
		if err := r.Put(r.key, bytes.NewBufferString("fresh")); err != nil {
			r.t.Errorf("replacing %s: %v", r.key, err)
		}
	}
	return r.Local.ListWithOptions(ctx, opts)
}

// backdate rewrites key's metadata sidecar with md.
func backdate(t *testing.T, dir, key string, md *common.Metadata) {
	t.Helper()
	data, err := json.Marshal(md)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(key))+common.MetadataSuffix, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// plainStorage hides every optional interface of the backend it wraps, so
// ApplyPolicies takes its generic re-check path.
type plainStorage struct {
	common.Storage
	lister *replacingLister
}

func (p plainStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return p.lister.ListWithOptions(ctx, opts)
}

// An expired object on the first listing page that is replaced while later
// pages are listed is not deleted: the action applies only to the revision
// the listing showed (#224).
func TestApplyPolicies_SkipsObjectReplacedDuringListing(t *testing.T) {
	for _, conditional := range []bool{true, false} {
		t.Run(fmt.Sprintf("conditional=%v", conditional), func(t *testing.T) {
			old := applyPoliciesPageSize
			applyPoliciesPageSize = 2
			t.Cleanup(func() { applyPoliciesPageSize = old })

			dir := t.TempDir()
			store, ok := local.New().(*local.Local)
			if !ok {
				t.Fatal("local.New() is not a *local.Local")
			}
			if err := store.Configure(map[string]string{"path": dir}, common.Credentials{}); err != nil {
				t.Fatal(err)
			}
			past := time.Now().Add(-48 * time.Hour)
			for i := range 5 {
				key := fmt.Sprintf("logs/%02d.txt", i)
				if err := store.Put(key, bytes.NewBufferString("old")); err != nil {
					t.Fatal(err)
				}
				if err := store.UpdateMetadata(context.Background(), key, &common.Metadata{}); err != nil {
					t.Fatal(err)
				}
				// Backdate the sidecar's LastModified, the retention clock.
				md, err := store.GetMetadata(context.Background(), key)
				if err != nil {
					t.Fatal(err)
				}
				md.LastModified = past
				backdate(t, dir, key, md)
			}
			if err := store.AddPolicy(common.LifecyclePolicy{ID: "logs", Prefix: "logs/", Retention: time.Hour, Action: "delete"}); err != nil {
				t.Fatal(err)
			}

			lister := &replacingLister{Local: store, t: t, key: "logs/00.txt"}
			var backend common.Storage = lister
			if !conditional {
				backend = plainStorage{Storage: store, lister: lister}
			}
			Reset()
			t.Cleanup(Reset)
			if err := Initialize(&FacadeConfig{
				Backends:       map[string]common.Storage{"loc": backend},
				DefaultBackend: "loc",
			}); err != nil {
				t.Fatalf("Initialize: %v", err)
			}

			var failures []string
			result, err := ApplyPolicies(context.Background(), "loc", func(key, _ string, err error) {
				failures = append(failures, key+": "+err.Error())
			})
			if err != nil {
				t.Fatalf("ApplyPolicies: %v", err)
			}
			if len(failures) != 0 {
				t.Fatalf("unexpected action failures: %v", failures)
			}
			if result.ObjectsProcessed != 4 {
				t.Errorf("ObjectsProcessed = %d, want 4", result.ObjectsProcessed)
			}
			r, err := store.Get("logs/00.txt")
			if err != nil {
				t.Fatalf("the replacement was deleted: %v", err)
			}
			data, _ := io.ReadAll(r)
			_ = r.Close()
			if string(data) != "fresh" {
				t.Errorf("logs/00.txt = %q, want the replacement", data)
			}
			for i := 1; i < 5; i++ {
				key := fmt.Sprintf("logs/%02d.txt", i)
				if _, err := os.Stat(filepath.Join(dir, key)); !os.IsNotExist(err) {
					t.Errorf("%s: expected deleted, got err=%v", key, err)
				}
			}
		})
	}
}
