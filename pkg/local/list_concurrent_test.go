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

package local_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

// TestListConcurrentWithPut asserts that listing never fails, and never
// returns an in-flight .tmp-* name, while Put is creating and renaming its
// temporary files in the directories being walked (go-objstore#68).
func TestListConcurrentWithPut(t *testing.T) {
	tempDir := t.TempDir()

	storage := local.New()
	if err := storage.Configure(map[string]string{"path": tempDir}, noCreds); err != nil {
		t.Fatalf("Configure failed: %v", err)
	}

	const stable = "stable/anchor"
	if err := storage.Put(stable, bytes.NewReader([]byte("anchor"))); err != nil {
		t.Fatalf("seed Put failed: %v", err)
	}

	const (
		writers    = 4
		listers    = 4
		putsPerKey = 150
	)

	var (
		wg      sync.WaitGroup
		stop    atomic.Bool
		writing sync.WaitGroup
	)
	errs := make(chan error, writers+2*listers)
	report := func(err error) {
		select {
		case errs <- err:
		default:
		}
	}

	checkKeys := func(keys []string) error {
		found := false
		for _, k := range keys {
			if strings.Contains(k, ".tmp-") {
				return fmt.Errorf("in-flight temp name returned as key: %q", k)
			}
			if k == stable {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("stable key %q missing from listing", stable)
		}
		return nil
	}

	for w := 0; w < writers; w++ {
		writing.Add(1)
		go func(w int) {
			defer writing.Done()
			for i := 0; i < putsPerKey; i++ {
				// Overwrite a small key set (and write into the same directory
				// as the anchor) so temp files churn in directories under walk.
				key := fmt.Sprintf("stable/w%d-k%d", w, i%5)
				if err := storage.Put(key, bytes.NewReader([]byte("payload"))); err != nil {
					report(fmt.Errorf("Put(%q): %w", key, err))
					return
				}
			}
		}(w)
	}

	for n := 0; n < listers; n++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				keys, err := storage.List("")
				if err != nil {
					report(fmt.Errorf("List: %w", err))
					return
				}
				if err := checkKeys(keys); err != nil {
					report(fmt.Errorf("List: %w", err))
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for !stop.Load() {
				res, err := storage.ListWithOptions(context.Background(), &common.ListOptions{MaxResults: 10000})
				if err != nil {
					report(fmt.Errorf("ListWithOptions: %w", err))
					return
				}
				keys := make([]string, 0, len(res.Objects))
				for _, o := range res.Objects {
					keys = append(keys, o.Key)
				}
				if err := checkKeys(keys); err != nil {
					report(fmt.Errorf("ListWithOptions: %w", err))
					return
				}
			}
		}()
	}

	writing.Wait()
	stop.Store(true)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}
