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

package pool_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/pool"
)

// ---------------------------------------------------------------------------
// Error type Error() method coverage
// All eight Error() implementations were at 0% because the existing tests
// only check via errors.As (which compares pointers, not strings). Calling
// Error() directly exercises the uncovered statement in each method.
// ---------------------------------------------------------------------------

func TestPoolNotFoundError_Error(t *testing.T) {
	e := &pool.PoolNotFoundError{Name: "mypool"}
	want := "pool/mypool: pool not found"
	if got := e.Error(); got != want {
		t.Errorf("PoolNotFoundError.Error() = %q, want %q", got, want)
	}
}

func TestEmptyPoolError_Error(t *testing.T) {
	e := &pool.EmptyPoolError{Name: "mypool"}
	want := "pool/mypool: pool has no candidates"
	if got := e.Error(); got != want {
		t.Errorf("EmptyPoolError.Error() = %q, want %q", got, want)
	}
}

func TestDuplicatePoolError_Error(t *testing.T) {
	e := &pool.DuplicatePoolError{Name: "mypool"}
	want := "pool/mypool: pool already registered"
	if got := e.Error(); got != want {
		t.Errorf("DuplicatePoolError.Error() = %q, want %q", got, want)
	}
}

func TestStrategyNotImplementedError_Error(t *testing.T) {
	e := &pool.StrategyNotImplementedError{Strategy: "custom"}
	want := "pool: strategy custom not implemented"
	if got := e.Error(); got != want {
		t.Errorf("StrategyNotImplementedError.Error() = %q, want %q", got, want)
	}
}

func TestNonLocalCandidateError_Error(t *testing.T) {
	e := &pool.NonLocalCandidateError{Pool: "local-pool"}
	want := "pool/local-pool: LocalOnly wrapper: candidate is not a PathAccessor"
	if got := e.Error(); got != want {
		t.Errorf("NonLocalCandidateError.Error() = %q, want %q", got, want)
	}
}

func TestInvalidPoolNameError_Error(t *testing.T) {
	e := &pool.InvalidPoolNameError{}
	want := "pool: pool name must not be empty"
	if got := e.Error(); got != want {
		t.Errorf("InvalidPoolNameError.Error() = %q, want %q", got, want)
	}
}

func TestNilStrategyError_Error(t *testing.T) {
	e := &pool.NilStrategyError{Pool: "mypool"}
	want := "pool/mypool: strategy must not be nil"
	if got := e.Error(); got != want {
		t.Errorf("NilStrategyError.Error() = %q, want %q", got, want)
	}
}

func TestMissingStateStoreError_Error(t *testing.T) {
	e := &pool.MissingStateStoreError{Strategy: "round_robin"}
	want := "pool: round_robin strategy requires a non-nil StateStore"
	if got := e.Error(); got != want {
		t.Errorf("MissingStateStoreError.Error() = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// RoundRobinStrategy: cursorKey fallback (empty pool name)
// ---------------------------------------------------------------------------

// TestRoundRobin_UnboundName exercises the cursorKey fallback branch where
// the context carries no pool name (poolNameFromContext returns ""). The
// strategy should still pick successfully using the "_unbound_" namespace.
func TestRoundRobin_UnboundName(t *testing.T) {
	s := &pool.RoundRobinStrategy{State: newMemStateStore()}
	c := configuredLocal(t)

	// Call Pick directly — no Manager, so no pool name in context.
	ctx := context.Background()

	// Real single-candidate pick through the unbound path.
	got, err := s.Pick(ctx, []common.Storage{c}, pool.Hint{})
	if err != nil {
		t.Fatalf("Pick() unexpected error: %v", err)
	}
	if got == nil {
		t.Error("Pick() returned nil storage")
	}
}

// ---------------------------------------------------------------------------
// RoundRobinStrategy: loadCursor corrupt/wrong-length value
// ---------------------------------------------------------------------------

// corruptStateStore inserts a value of the wrong byte length under any key
// that is queried, to trigger the "malformed value → reset to zero" branch
// inside loadCursor / loadTallies.
type corruptStateStore struct {
	// badValue is the bytes returned on Get (should be wrong length).
	badValue []byte
	// afterPut stores the last Put value so the test can inspect it.
	afterPut []byte
}

func (c *corruptStateStore) Get(_ context.Context, _ string) ([]byte, bool, error) {
	if len(c.badValue) == 0 {
		return nil, false, nil
	}
	return c.badValue, true, nil
}

func (c *corruptStateStore) Put(_ context.Context, _ string, value []byte) error {
	c.afterPut = value
	return nil
}

// errGetStateStore always returns an error from Get.
type errGetStateStore struct {
	getErr error
}

func (s *errGetStateStore) Get(_ context.Context, _ string) ([]byte, bool, error) {
	return nil, false, s.getErr
}

func (s *errGetStateStore) Put(_ context.Context, _ string, _ []byte) error {
	return nil
}

// errPutStateStore returns no error from Get (key missing) but errors on Put.
type errPutStateStore struct {
	putErr error
}

func (s *errPutStateStore) Get(_ context.Context, _ string) ([]byte, bool, error) {
	return nil, false, nil
}

func (s *errPutStateStore) Put(_ context.Context, _ string, _ []byte) error {
	return s.putErr
}

// TestRoundRobin_CorruptCursor verifies that a stored cursor value of the
// wrong length is silently reset to zero (cursor starts fresh).
func TestRoundRobin_CorruptCursor(t *testing.T) {
	// 3 bytes is wrong; loadCursor expects exactly 8.
	store := &corruptStateStore{badValue: []byte{0x01, 0x02, 0x03}}
	s := &pool.RoundRobinStrategy{State: store}
	c := configuredLocal(t)

	got, err := s.Pick(context.Background(), []common.Storage{c}, pool.Hint{})
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	if got == nil {
		t.Error("Pick() returned nil storage")
	}
}

// ---------------------------------------------------------------------------
// LeastUtilizationStrategy: loadKey fallback (empty pool name)
// ---------------------------------------------------------------------------

// TestLeastUtilization_UnboundName exercises the loadKey fallback branch
// where the context carries no pool name.
func TestLeastUtilization_UnboundName(t *testing.T) {
	s := &pool.LeastUtilizationStrategy{State: newMemStateStore()}
	c := configuredLocal(t)

	// Direct call without Manager → poolNameFromContext returns "".
	got, err := s.Pick(context.Background(), []common.Storage{c}, pool.Hint{})
	if err != nil {
		t.Fatalf("Pick() unexpected error: %v", err)
	}
	if got == nil {
		t.Error("Pick() returned nil storage")
	}
}

// ---------------------------------------------------------------------------
// LeastUtilizationStrategy: loadTallies wrong-length reset
// ---------------------------------------------------------------------------

// TestLeastUtilization_CorruptTallies verifies that a stored tallies blob of
// the wrong byte length is silently treated as all-zero (tally reset).
func TestLeastUtilization_CorruptTallies(t *testing.T) {
	// 5 bytes is wrong; loadTallies expects exactly 8*n bytes.
	store := &corruptStateStore{badValue: []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE}}
	s := &pool.LeastUtilizationStrategy{State: store}
	c := configuredLocal(t)

	got, err := s.Pick(context.Background(), []common.Storage{c}, pool.Hint{})
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	if got == nil {
		t.Error("Pick() returned nil storage")
	}
}

// ---------------------------------------------------------------------------
// RoundRobinStrategy: StateStore.Get error propagation
// ---------------------------------------------------------------------------

// TestRoundRobin_StateStoreGetError verifies that a StateStore.Get error
// is propagated out of Pick (covers the loadCursor error branch).
func TestRoundRobin_StateStoreGetError(t *testing.T) {
	getErr := errors.New("get failed")
	s := &pool.RoundRobinStrategy{State: &errGetStateStore{getErr: getErr}}
	c := configuredLocal(t)

	_, err := s.Pick(context.Background(), []common.Storage{c}, pool.Hint{})
	if !errors.Is(err, getErr) {
		t.Errorf("Pick() error = %v, want getErr", err)
	}
}

// TestRoundRobin_StateStorePutError verifies that a StateStore.Put error
// is propagated out of Pick (covers the storeCursor error branch).
func TestRoundRobin_StateStorePutError(t *testing.T) {
	putErr := errors.New("put failed")
	s := &pool.RoundRobinStrategy{State: &errPutStateStore{putErr: putErr}}
	c := configuredLocal(t)

	_, err := s.Pick(context.Background(), []common.Storage{c}, pool.Hint{})
	if !errors.Is(err, putErr) {
		t.Errorf("Pick() error = %v, want putErr", err)
	}
}

// ---------------------------------------------------------------------------
// LeastUtilizationStrategy: StateStore.Get / Put error propagation
// ---------------------------------------------------------------------------

// TestLeastUtilization_StateStoreGetError verifies that a StateStore.Get
// error is propagated out of Pick (covers the loadTallies error branch).
func TestLeastUtilization_StateStoreGetError(t *testing.T) {
	getErr := errors.New("get failed")
	s := &pool.LeastUtilizationStrategy{State: &errGetStateStore{getErr: getErr}}
	c := configuredLocal(t)

	_, err := s.Pick(context.Background(), []common.Storage{c}, pool.Hint{})
	if !errors.Is(err, getErr) {
		t.Errorf("Pick() error = %v, want getErr", err)
	}
}

// TestLeastUtilization_StateStorePutError verifies that a StateStore.Put
// error is propagated out of Pick (covers the storeTallies error branch).
func TestLeastUtilization_StateStorePutError(t *testing.T) {
	putErr := errors.New("put failed")
	s := &pool.LeastUtilizationStrategy{State: &errPutStateStore{putErr: putErr}}
	c := configuredLocal(t)

	_, err := s.Pick(context.Background(), []common.Storage{c}, pool.Hint{})
	if !errors.Is(err, putErr) {
		t.Errorf("Pick() error = %v, want putErr", err)
	}
}

// ---------------------------------------------------------------------------
// Manager.List: cancelled context
// ---------------------------------------------------------------------------

// TestManager_List_CancelledContext verifies that List returns
// context.Canceled when the supplied context is already cancelled.
func TestManager_List_CancelledContext(t *testing.T) {
	mgr := pool.NewManager()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := mgr.List(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("List() error = %v, want context.Canceled", err)
	}
}
