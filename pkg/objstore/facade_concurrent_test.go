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
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// concurrentTestTimeout bounds every concurrency test in this file.
const concurrentTestTimeout = 2 * time.Second

// resetFacade resets the package-level facade now and when the test ends.
func resetFacade(t *testing.T) {
	t.Helper()
	Reset()
	t.Cleanup(Reset)
}

// twoMemoryBackends returns a config with the memory backends "a" and "b",
// "a" being the default.
func twoMemoryBackends() *FacadeConfig {
	return &FacadeConfig{
		BackendConfigs: map[string]BackendConfig{
			"a": {Type: "memory", Settings: map[string]string{}},
			"b": {Type: "memory", Settings: map[string]string{}},
		},
		DefaultBackend: "a",
	}
}

// waitOrTimeout waits for wg, failing the test if ctx expires first.
func waitOrTimeout(ctx context.Context, t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("goroutines did not finish: %v", ctx.Err())
	}
}

func TestConcurrentInitialize(t *testing.T) {
	resetFacade(t)
	ctx, cancel := context.WithTimeout(context.Background(), concurrentTestTimeout)
	defer cancel()

	const workers = 32
	config := twoMemoryBackends()
	errs := make([]error, workers)
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = Initialize(config)
		}(i)
	}
	close(start)
	waitOrTimeout(ctx, t, &wg)

	for i, err := range errs {
		if err != nil {
			t.Errorf("Initialize() in goroutine %d: %v", i, err)
		}
	}
	if names := Backends(); len(names) != 2 {
		t.Fatalf("Backends() = %v, want two names", names)
	}

	a1, err := Backend("a")
	if err != nil {
		t.Fatalf("Backend(a): %v", err)
	}
	b1, err := Backend("b")
	if err != nil {
		t.Fatalf("Backend(b): %v", err)
	}

	// A further Initialize is a no-op: the backends stay the same instances.
	if err := Initialize(twoMemoryBackends()); err != nil {
		t.Fatalf("Initialize() again: %v", err)
	}
	a2, _ := Backend("a")
	b2, _ := Backend("b")
	if a1 != a2 || b1 != b2 {
		t.Error("backends were replaced by a later Initialize()")
	}
	def, err := DefaultBackend()
	if err != nil {
		t.Fatalf("DefaultBackend(): %v", err)
	}
	if def != a1 {
		t.Error("DefaultBackend() is not backend a")
	}
}

func TestConcurrentInitializeFailure(t *testing.T) {
	resetFacade(t)
	ctx, cancel := context.WithTimeout(context.Background(), concurrentTestTimeout)
	defer cancel()

	const workers = 16
	config := &FacadeConfig{
		BackendConfigs: map[string]BackendConfig{
			"bad": {Type: "nonexistent-backend-type", Settings: map[string]string{}},
		},
	}
	errs := make([]error, workers)
	start := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = Initialize(config)
		}(i)
	}
	close(start)
	waitOrTimeout(ctx, t, &wg)

	for i, err := range errs {
		if !errors.Is(err, ErrInitializationFailed) {
			t.Errorf("Initialize() in goroutine %d = %v, want ErrInitializationFailed", i, err)
		}
		if err != errs[0] { //nolint:errorlint // identity of the cached error is the point
			t.Errorf("Initialize() in goroutine %d returned a different error value", i)
		}
	}
	if IsInitialized() {
		t.Error("IsInitialized() = true after a failed Initialize()")
	}
}

// keyRef returns the reference goroutine n uses: odd goroutines address
// backend b explicitly, even ones the default backend.
func keyRef(n int, suffix string) (ref, key string, onB bool) {
	key = fmt.Sprintf("k%d%s", n, suffix)
	if n%2 == 1 {
		return "b:" + key, key, true
	}
	return key, key, false
}

// put writes through Put for a plain key (Put addresses the default backend
// only) and through PutWithContext for a "backend:key" reference.
func put(ctx context.Context, ref string, data io.Reader) error {
	if strings.Contains(ref, ":") {
		return PutWithContext(ctx, ref, data)
	}
	return Put(ref, data)
}

// get reads through Get for a plain key and GetWithContext for a reference.
func get(ctx context.Context, ref string) (io.ReadCloser, error) {
	if strings.Contains(ref, ":") {
		return GetWithContext(ctx, ref)
	}
	return Get(ref)
}

func TestConcurrentRoutedOperations(t *testing.T) {
	resetFacade(t)
	if err := Initialize(twoMemoryBackends()); err != nil {
		t.Fatalf("Initialize(): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), concurrentTestTimeout)
	defer cancel()

	const workers = 16
	errCh := make(chan error, workers*16)
	report := func(n int, op string, err error) {
		if err != nil && !errors.Is(err, common.ErrKeyNotFound) {
			errCh <- fmt.Errorf("goroutine %d %s: %w", n, op, err)
		}
	}

	var wg sync.WaitGroup
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ref, _, onB := keyRef(n, "")
			keepRef, _, _ := keyRef(n, "-keep")
			payload := []byte(fmt.Sprintf("payload-%d", n))

			report(n, "Put", put(ctx, ref, bytes.NewReader(payload)))
			report(n, "Put keep", put(ctx, keepRef, bytes.NewReader(payload)))
			report(n, "PutWithMetadata", PutWithMetadata(ctx, ref, bytes.NewReader(payload),
				&common.Metadata{ContentType: "text/plain"}))

			rc, err := get(ctx, ref)
			report(n, "Get", err)
			if err == nil {
				got, rerr := io.ReadAll(rc)
				report(n, "read", rerr)
				report(n, "close", rc.Close())
				if rerr == nil && !bytes.Equal(got, payload) {
					errCh <- fmt.Errorf("goroutine %d Get = %q, want %q", n, got, payload)
				}
			}

			_, err = GetMetadata(ctx, ref)
			report(n, "GetMetadata", err)

			ok, err := Exists(ctx, ref)
			report(n, "Exists", err)
			if err == nil && !ok {
				errCh <- fmt.Errorf("goroutine %d Exists(%s) = false", n, ref)
			}

			_, err = List("k")
			report(n, "List", err)

			backendName := ""
			if onB {
				backendName = "b"
			}
			_, err = ListWithOptions(ctx, backendName, &common.ListOptions{Prefix: "k"})
			report(n, "ListWithOptions", err)

			report(n, "DeleteWithContext", DeleteWithContext(ctx, ref))
		}(n)
	}
	waitOrTimeout(ctx, t, &wg)
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	a, err := DefaultBackend()
	if err != nil {
		t.Fatalf("DefaultBackend(): %v", err)
	}
	b, err := Backend("b")
	if err != nil {
		t.Fatalf("Backend(b): %v", err)
	}
	for n := 0; n < workers; n++ {
		_, key, onB := keyRef(n, "-keep")
		want, other := a, b
		if onB {
			want, other = b, a
		}
		if ok, err := want.Exists(ctx, key); err != nil || !ok {
			t.Errorf("key %s missing on its backend (exists=%v, err=%v)", key, ok, err)
		}
		if ok, err := other.Exists(ctx, key); err != nil || ok {
			t.Errorf("key %s found on the other backend (exists=%v, err=%v)", key, ok, err)
		}
		_, deleted, _ := keyRef(n, "")
		if ok, _ := want.Exists(ctx, deleted); ok {
			t.Errorf("key %s still exists after DeleteWithContext", deleted)
		}
	}
}

func TestConcurrentBackendLookups(t *testing.T) {
	resetFacade(t)
	if err := Initialize(twoMemoryBackends()); err != nil {
		t.Fatalf("Initialize(): %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), concurrentTestTimeout)
	defer cancel()

	const readers, writers, rounds = 32, 8, 50
	errCh := make(chan error, (readers+writers)*rounds)

	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for r := 0; r < rounds && ctx.Err() == nil; r++ {
				if _, err := Backend("b"); err != nil {
					errCh <- fmt.Errorf("reader %d Backend: %w", i, err)
				}
				if _, err := DefaultBackend(); err != nil {
					errCh <- fmt.Errorf("reader %d DefaultBackend: %w", i, err)
				}
				if names := Backends(); len(names) != 2 {
					errCh <- fmt.Errorf("reader %d Backends() = %v", i, names)
				}
				if !IsInitialized() {
					errCh <- fmt.Errorf("reader %d IsInitialized() = false", i)
				}
			}
		}(i)
	}
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for r := 0; r < rounds && ctx.Err() == nil; r++ {
				ref, _, _ := keyRef(i, fmt.Sprintf("-r%d", r))
				if err := put(ctx, ref, strings.NewReader("v")); err != nil {
					errCh <- fmt.Errorf("writer %d Put: %w", i, err)
					continue
				}
				rc, err := get(ctx, ref)
				if err != nil {
					errCh <- fmt.Errorf("writer %d Get: %w", i, err)
					continue
				}
				_, _ = io.Copy(io.Discard, rc)
				_ = rc.Close()
			}
		}(i)
	}
	waitOrTimeout(ctx, t, &wg)
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
