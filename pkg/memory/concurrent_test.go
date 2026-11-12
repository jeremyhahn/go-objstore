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

package memory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// concurrentTimeout bounds every concurrency test in this file.
const concurrentTimeout = 2 * time.Second

// errCollector keeps the first errors reported by goroutines without blocking.
type errCollector struct {
	ch chan error
}

func newErrCollector(n int) *errCollector {
	return &errCollector{ch: make(chan error, n)}
}

func (c *errCollector) report(err error) {
	select {
	case c.ch <- err:
	default:
	}
}

func (c *errCollector) drain(t *testing.T) {
	t.Helper()
	close(c.ch)
	for err := range c.ch {
		t.Error(err)
	}
}

// isNotFound reports whether err is the not-found error a concurrent
// Delete may cause: ErrKeyNotFound, or ErrMetadataNotFound from GetMetadata.
func isNotFound(err error) bool {
	return errors.Is(err, common.ErrKeyNotFound) || errors.Is(err, common.ErrMetadataNotFound)
}

// TestConcurrentPutGetSameKey asserts that readers of one key only ever see
// one complete payload written by a writer: never a torn mix of two, and
// never an empty or missing object once the first write has landed.
func TestConcurrentPutGetSameKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), concurrentTimeout)
	defer cancel()

	const (
		writers  = 16
		readers  = 16
		putsEach = 50
		size     = 4096
		key      = "shared/object"
	)

	m := New()
	payloads := make(map[string]struct{}, writers)
	data := make([][]byte, writers)
	for w := 0; w < writers; w++ {
		data[w] = bytes.Repeat([]byte{byte('A' + w)}, size)
		payloads[string(data[w])] = struct{}{}
	}

	// The first write lands before any reader starts.
	if err := m.PutWithContext(ctx, key, bytes.NewReader(data[0])); err != nil {
		t.Fatalf("seed Put: %v", err)
	}

	errs := newErrCollector(writers + readers)
	var (
		writing sync.WaitGroup
		reading sync.WaitGroup
		stop    atomic.Bool
	)

	for w := 0; w < writers; w++ {
		writing.Add(1)
		go func(w int) {
			defer writing.Done()
			for i := 0; i < putsEach && ctx.Err() == nil; i++ {
				if err := m.PutWithContext(ctx, key, bytes.NewReader(data[w])); err != nil {
					errs.report(fmt.Errorf("writer %d: Put: %w", w, err))
					return
				}
			}
		}(w)
	}

	for r := 0; r < readers; r++ {
		reading.Add(1)
		go func(r int) {
			defer reading.Done()
			for !stop.Load() && ctx.Err() == nil {
				rc, err := m.GetWithContext(ctx, key)
				if err != nil {
					errs.report(fmt.Errorf("reader %d: Get: %w", r, err))
					return
				}
				got, err := io.ReadAll(rc)
				_ = rc.Close()
				if err != nil {
					errs.report(fmt.Errorf("reader %d: read: %w", r, err))
					return
				}
				if _, ok := payloads[string(got)]; !ok {
					errs.report(fmt.Errorf("reader %d: torn or unknown payload of %d bytes", r, len(got)))
					return
				}
			}
		}(r)
	}

	writing.Wait()
	stop.Store(true)
	reading.Wait()
	errs.drain(t)
	if err := ctx.Err(); err != nil {
		t.Fatalf("test exceeded %v: %v", concurrentTimeout, err)
	}
}

// TestConcurrentMixedOperations runs metadata, existence, delete and list
// operations over an overlapping key set from many goroutines. The only
// error allowed is the not-found error a concurrent Delete can cause.
// UpdateMetadata is not part of the mix yet: GetMetadata dereferences the
// stored metadata pointer after releasing the lock, and UpdateMetadata swaps
// that pointer, which races until memory.go is fixed.
func TestConcurrentMixedOperations(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), concurrentTimeout)
	defer cancel()

	const (
		workers = 8
		rounds  = 100
		keys    = 6
	)

	m := New()
	keyName := func(i int) string { return fmt.Sprintf("mixed/k%d", i%keys) }

	errs := newErrCollector(workers * 8)
	check := func(op, key string, err error) bool {
		if err == nil || isNotFound(err) {
			return true
		}
		errs.report(fmt.Errorf("%s(%q): %w", op, key, err))
		return false
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(6)

		go func(w int) { // PutWithMetadata
			defer wg.Done()
			for i := 0; i < rounds && ctx.Err() == nil; i++ {
				key := keyName(w + i)
				meta := &common.Metadata{
					ContentType: "text/plain",
					Custom:      map[string]string{"writer": fmt.Sprint(w), "round": fmt.Sprint(i)},
				}
				payload := []byte(fmt.Sprintf("w%d-r%d", w, i))
				if !check("PutWithMetadata", key, m.PutWithMetadata(ctx, key, bytes.NewReader(payload), meta)) {
					return
				}
			}
		}(w)

		go func(w int) { // GetMetadata
			defer wg.Done()
			for i := 0; i < rounds && ctx.Err() == nil; i++ {
				key := keyName(w + i + 1)
				meta, err := m.GetMetadata(ctx, key)
				if !check("GetMetadata", key, err) {
					return
				}
				if err == nil && meta == nil {
					errs.report(fmt.Errorf("GetMetadata(%q): nil metadata without error", key))
					return
				}
			}
		}(w)

		go func(w int) { // Exists
			defer wg.Done()
			for i := 0; i < rounds && ctx.Err() == nil; i++ {
				key := keyName(w + i + 3)
				_, err := m.Exists(ctx, key)
				if !check("Exists", key, err) {
					return
				}
			}
		}(w)

		go func(w int) { // Delete
			defer wg.Done()
			for i := 0; i < rounds && ctx.Err() == nil; i++ {
				key := keyName(w + i + 4)
				if !check("Delete", key, m.DeleteWithContext(ctx, key)) {
					return
				}
			}
		}(w)

		go func() { // List
			defer wg.Done()
			for i := 0; i < rounds && ctx.Err() == nil; i++ {
				if _, err := m.ListWithContext(ctx, "mixed/"); !check("List", "mixed/", err) {
					return
				}
			}
		}()

		go func() { // ListWithOptions, paging through every page
			defer wg.Done()
			for i := 0; i < rounds && ctx.Err() == nil; i++ {
				opts := &common.ListOptions{Prefix: "mixed/", MaxResults: 2}
				for page := 0; page <= keys; page++ {
					res, err := m.ListWithOptions(ctx, opts)
					if !check("ListWithOptions", opts.ContinueFrom, err) {
						return
					}
					for _, obj := range res.Objects {
						if obj.Metadata == nil {
							errs.report(fmt.Errorf("ListWithOptions: %q has nil metadata", obj.Key))
							return
						}
					}
					if !res.Truncated {
						break
					}
					opts.ContinueFrom = res.NextToken
				}
			}
		}()
	}

	wg.Wait()
	errs.drain(t)
	if err := ctx.Err(); err != nil {
		t.Fatalf("test exceeded %v: %v", concurrentTimeout, err)
	}
}

// TestConcurrentGetMetadataCopy asserts that a caller mutating the Custom map
// GetMetadata returned does not race with another goroutine reading the same
// key's metadata: GetMetadata hands out a copy.
func TestConcurrentGetMetadataCopy(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), concurrentTimeout)
	defer cancel()

	const (
		key    = "copy/object"
		rounds = 500
	)

	m := New()
	seed := &common.Metadata{Custom: map[string]string{"owner": "seed"}}
	if err := m.PutWithMetadata(ctx, key, bytes.NewReader([]byte("payload")), seed); err != nil {
		t.Fatalf("seed PutWithMetadata: %v", err)
	}

	errs := newErrCollector(4)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() { // mutates the copy it was given
		defer wg.Done()
		for i := 0; i < rounds && ctx.Err() == nil; i++ {
			meta, err := m.GetMetadata(ctx, key)
			if err != nil {
				errs.report(fmt.Errorf("mutator: GetMetadata: %w", err))
				return
			}
			meta.Custom["owner"] = fmt.Sprintf("mutator-%d", i)
			meta.Custom[fmt.Sprintf("extra-%d", i)] = "x"
			delete(meta.Custom, "owner")
		}
	}()

	go func() { // reads the stored metadata
		defer wg.Done()
		for i := 0; i < rounds && ctx.Err() == nil; i++ {
			meta, err := m.GetMetadata(ctx, key)
			if err != nil {
				errs.report(fmt.Errorf("reader: GetMetadata: %w", err))
				return
			}
			if got := meta.Custom["owner"]; got != "seed" {
				errs.report(fmt.Errorf("reader: owner = %q, want %q", got, "seed"))
				return
			}
			if len(meta.Custom) != 1 {
				errs.report(fmt.Errorf("reader: Custom has %d entries, want 1", len(meta.Custom)))
				return
			}
		}
	}()

	wg.Wait()
	errs.drain(t)
	if err := ctx.Err(); err != nil {
		t.Fatalf("test exceeded %v: %v", concurrentTimeout, err)
	}
}
