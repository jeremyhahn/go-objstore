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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

// TestApplyPolicies_PagesThroughListing puts more expired objects than one
// list page holds and checks that ApplyPolicies processes every one of them,
// not only the first page (#224).
func TestApplyPolicies_PagesThroughListing(t *testing.T) {
	old := applyPoliciesPageSize
	applyPoliciesPageSize = 4
	t.Cleanup(func() { applyPoliciesPageSize = old })

	dir := t.TempDir()
	store := local.New()
	if err := store.Configure(map[string]string{"path": dir}, common.Credentials{}); err != nil {
		t.Fatal(err)
	}
	Reset()
	t.Cleanup(Reset)
	if err := Initialize(&FacadeConfig{
		Backends:       map[string]common.Storage{"loc": store},
		DefaultBackend: "loc",
	}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	// Old objects are written straight to disk with no metadata sidecar, so
	// the listing reports the file's (backdated) modification time.
	past := time.Now().Add(-48 * time.Hour)
	writeOld := func(key string) {
		t.Helper()
		path := filepath.Join(dir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, past, past); err != nil {
			t.Fatal(err)
		}
	}
	const expired = 15
	for i := range expired {
		writeOld(fmt.Sprintf("logs/%02d.txt", i))
	}
	// A recent object under the prefix, and an old one outside it, stay.
	if err := store.Put("logs/zz-new.txt", bytes.NewBufferString("x")); err != nil {
		t.Fatal(err)
	}
	writeOld("data/old.txt")
	keep := []string{"logs/zz-new.txt", "data/old.txt"}
	if err := store.AddPolicy(common.LifecyclePolicy{ID: "logs", Prefix: "logs/", Retention: time.Hour, Action: "delete"}); err != nil {
		t.Fatal(err)
	}

	var failures []string
	result, err := ApplyPolicies(context.Background(), "loc", func(key, action string, err error) {
		failures = append(failures, key+": "+err.Error())
	})
	if err != nil {
		t.Fatalf("ApplyPolicies: %v", err)
	}
	if len(failures) != 0 {
		t.Fatalf("unexpected action failures: %v", failures)
	}
	if result.PoliciesCount != 1 || result.ObjectsProcessed != expired {
		t.Fatalf("result = %+v, want 1 policy and %d objects processed", *result, expired)
	}
	for i := range expired {
		key := fmt.Sprintf("logs/%02d.txt", i)
		if _, err := os.Stat(filepath.Join(dir, key)); !os.IsNotExist(err) {
			t.Errorf("%s: expected deleted, got err=%v", key, err)
		}
	}
	for _, key := range keep {
		if _, err := os.Stat(filepath.Join(dir, key)); err != nil {
			t.Errorf("%s: expected kept, got err=%v", key, err)
		}
	}
}

// TestApplyPolicies_NoPolicies returns a zero result without listing.
func TestApplyPolicies_NoPolicies(t *testing.T) {
	initBucketFacade(t)
	result, err := ApplyPolicies(context.Background(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.PoliciesCount != 0 || result.ObjectsProcessed != 0 {
		t.Fatalf("result = %+v, want zero", *result)
	}
}

// TestApplyPolicies_UnknownBackend returns the facade's error.
func TestApplyPolicies_UnknownBackend(t *testing.T) {
	initBucketFacade(t)
	if _, err := ApplyPolicies(context.Background(), "nope", nil); err == nil {
		t.Fatal("expected an error for an unknown backend")
	}
}

// stalledListStorage reports every listing page truncated, with the same
// continuation token each time.
type stalledListStorage struct {
	*mockStorage
	token string
	calls int
}

func (s *stalledListStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return []common.LifecyclePolicy{{ID: "p", Prefix: "", Retention: time.Hour, Action: "delete"}}, nil
}

func (s *stalledListStorage) ListWithOptions(_ context.Context, _ *common.ListOptions) (*common.ListResult, error) {
	s.calls++
	if s.calls > 10 {
		return nil, fmt.Errorf("listed %d pages: the pass did not stop", s.calls)
	}
	return &common.ListResult{
		Objects:   []*common.ObjectInfo{{Key: "old", Metadata: &common.Metadata{LastModified: time.Now().Add(-48 * time.Hour)}}},
		Truncated: true,
		NextToken: s.token,
	}, nil
}

// A truncated listing that gives no new continuation token makes
// ApplyPolicies fail rather than loop or stop with the store half seen (#224).
func TestApplyPolicies_StalledListing(t *testing.T) {
	for _, token := range []string{"", "t1"} {
		t.Run(fmt.Sprintf("token %q", token), func(t *testing.T) {
			Reset()
			t.Cleanup(Reset)
			fake := &stalledListStorage{mockStorage: newMockStorage("stalled"), token: token}
			if err := Initialize(&FacadeConfig{
				Backends:       map[string]common.Storage{"stalled": fake},
				DefaultBackend: "stalled",
			}); err != nil {
				t.Fatal(err)
			}
			res, err := ApplyPolicies(context.Background(), "stalled", nil)
			if !errors.Is(err, ErrListingStalled) {
				t.Fatalf("ApplyPolicies() = %+v, %v; want ErrListingStalled", res, err)
			}
		})
	}
}
