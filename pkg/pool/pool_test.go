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
	"sync"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
	"github.com/jeremyhahn/go-objstore/pkg/memory"
	"github.com/jeremyhahn/go-objstore/pkg/pool"
)

// memStateStore is the in-memory StateStore stub used by tests. It is
// intentionally minimal — concurrent access is guarded by a mutex so
// the round-robin tests can run with -race cleanly.
type memStateStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemStateStore() *memStateStore {
	return &memStateStore{data: make(map[string][]byte)}
}

func (s *memStateStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	v, ok := s.data[key]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

func (s *memStateStore) Put(ctx context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	stored := make([]byte, len(value))
	copy(stored, value)
	s.data[key] = stored
	return nil
}

// configuredLocal returns a *local.Local Configure'd against a fresh temp
// directory. Returned as common.Storage to match the pool API surface.
func configuredLocal(t *testing.T) common.Storage {
	t.Helper()
	dir := t.TempDir()
	s := local.New()
	if err := s.Configure(map[string]string{"path": dir}); err != nil {
		t.Fatalf("Configure failed: %v", err)
	}
	return s
}

// localPath returns the LocalPath of a Storage, failing the test if it
// does not satisfy PathAccessor. Centralising the assertion keeps
// individual tests readable.
func localPath(t *testing.T, s common.Storage) string {
	t.Helper()
	pa, ok := s.(common.PathAccessor)
	if !ok {
		t.Fatalf("storage %T does not satisfy PathAccessor", s)
	}
	return pa.LocalPath()
}

// TestManager_Register_HappyPath registers a pool with three local
// candidates and verifies List reports it back.
func TestManager_Register_HappyPath(t *testing.T) {
	mgr := pool.NewManager()
	candidates := []common.Storage{
		configuredLocal(t),
		configuredLocal(t),
		configuredLocal(t),
	}
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}

	if err := mgr.Register("default", candidates, rr); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	pools, err := mgr.List(context.Background())
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("List returned %d pools, want 1", len(pools))
	}
	if pools[0].Name != "default" {
		t.Errorf("Pool name = %q, want %q", pools[0].Name, "default")
	}
	if len(pools[0].Candidates) != 3 {
		t.Errorf("Pool candidates = %d, want 3", len(pools[0].Candidates))
	}
	if pools[0].Strategy.Name() != "round_robin" {
		t.Errorf("Strategy name = %q, want round_robin", pools[0].Strategy.Name())
	}
}

// TestManager_Register_DuplicateName verifies a second Register with the
// same name returns DuplicatePoolError.
func TestManager_Register_DuplicateName(t *testing.T) {
	mgr := pool.NewManager()
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}
	candidates := []common.Storage{configuredLocal(t)}

	if err := mgr.Register("p1", candidates, rr); err != nil {
		t.Fatalf("first Register failed: %v", err)
	}

	err := mgr.Register("p1", candidates, rr)
	var dup *pool.DuplicatePoolError
	if !errors.As(err, &dup) {
		t.Fatalf("expected DuplicatePoolError, got %v", err)
	}
	if dup.Name != "p1" {
		t.Errorf("DuplicatePoolError.Name = %q, want %q", dup.Name, "p1")
	}
}

// TestManager_Register_EmptyName verifies an empty pool name is
// rejected with InvalidPoolNameError.
func TestManager_Register_EmptyName(t *testing.T) {
	mgr := pool.NewManager()
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}

	err := mgr.Register("", []common.Storage{configuredLocal(t)}, rr)
	var invalid *pool.InvalidPoolNameError
	if !errors.As(err, &invalid) {
		t.Fatalf("expected InvalidPoolNameError, got %v", err)
	}
}

// TestManager_Register_NilStrategy verifies a nil strategy is rejected
// with NilStrategyError.
func TestManager_Register_NilStrategy(t *testing.T) {
	mgr := pool.NewManager()

	err := mgr.Register("p1", []common.Storage{configuredLocal(t)}, nil)
	var nilStrat *pool.NilStrategyError
	if !errors.As(err, &nilStrat) {
		t.Fatalf("expected NilStrategyError, got %v", err)
	}
	if nilStrat.Pool != "p1" {
		t.Errorf("NilStrategyError.Pool = %q, want %q", nilStrat.Pool, "p1")
	}
}

// TestManager_Pick_RoundRobin verifies that three sequential picks on a
// pool of three candidates return each candidate exactly once, in
// order. The cursor is persisted via the in-memory StateStore.
func TestManager_Pick_RoundRobin(t *testing.T) {
	mgr := pool.NewManager()
	c1 := configuredLocal(t)
	c2 := configuredLocal(t)
	c3 := configuredLocal(t)
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}

	if err := mgr.Register("default", []common.Storage{c1, c2, c3}, rr); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	ctx := context.Background()
	wantPaths := []string{localPath(t, c1), localPath(t, c2), localPath(t, c3)}

	for i, want := range wantPaths {
		got, err := mgr.Pick(ctx, "default", pool.Hint{})
		if err != nil {
			t.Fatalf("Pick #%d failed: %v", i, err)
		}
		if localPath(t, got) != want {
			t.Errorf("Pick #%d returned path %q, want %q", i, localPath(t, got), want)
		}
	}

	// Fourth pick wraps around to candidate 0.
	got, err := mgr.Pick(ctx, "default", pool.Hint{})
	if err != nil {
		t.Fatalf("Pick wrap-around failed: %v", err)
	}
	if localPath(t, got) != wantPaths[0] {
		t.Errorf("Wrap-around pick returned %q, want %q", localPath(t, got), wantPaths[0])
	}
}

// TestManager_Pick_NotFound verifies an unregistered pool name returns
// PoolNotFoundError.
func TestManager_Pick_NotFound(t *testing.T) {
	mgr := pool.NewManager()

	_, err := mgr.Pick(context.Background(), "ghost", pool.Hint{})
	var pnf *pool.PoolNotFoundError
	if !errors.As(err, &pnf) {
		t.Fatalf("expected PoolNotFoundError, got %v", err)
	}
	if pnf.Name != "ghost" {
		t.Errorf("PoolNotFoundError.Name = %q, want %q", pnf.Name, "ghost")
	}
}

// TestManager_Pick_EmptyPool verifies that picking from a pool with no
// candidates returns EmptyPoolError.
func TestManager_Pick_EmptyPool(t *testing.T) {
	mgr := pool.NewManager()
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}
	if err := mgr.Register("empty", nil, rr); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	_, err := mgr.Pick(context.Background(), "empty", pool.Hint{})
	var ep *pool.EmptyPoolError
	if !errors.As(err, &ep) {
		t.Fatalf("expected EmptyPoolError, got %v", err)
	}
	if ep.Name != "empty" {
		t.Errorf("EmptyPoolError.Name = %q, want %q", ep.Name, "empty")
	}
}

// TestRoundRobin_CursorPersistence verifies that recreating the Manager
// with the same StateStore picks up the cursor where the previous
// Manager left off, instead of resetting to zero. This is the property
// that makes round-robin survive daemon restarts.
func TestRoundRobin_CursorPersistence(t *testing.T) {
	store := newMemStateStore()
	c1 := configuredLocal(t)
	c2 := configuredLocal(t)
	c3 := configuredLocal(t)
	candidates := []common.Storage{c1, c2, c3}

	// First Manager — pick once. Should return c1.
	mgr1 := pool.NewManager()
	if err := mgr1.Register("default", candidates, &pool.RoundRobinStrategy{State: store}); err != nil {
		t.Fatalf("Register on mgr1 failed: %v", err)
	}
	pick1, err := mgr1.Pick(context.Background(), "default", pool.Hint{})
	if err != nil {
		t.Fatalf("Pick on mgr1 failed: %v", err)
	}
	if localPath(t, pick1) != localPath(t, c1) {
		t.Fatalf("mgr1 Pick = %q, want %q", localPath(t, pick1), localPath(t, c1))
	}

	// Second Manager — different value but same StateStore. Cursor
	// must continue from 1, returning c2 next.
	mgr2 := pool.NewManager()
	if err := mgr2.Register("default", candidates, &pool.RoundRobinStrategy{State: store}); err != nil {
		t.Fatalf("Register on mgr2 failed: %v", err)
	}
	pick2, err := mgr2.Pick(context.Background(), "default", pool.Hint{})
	if err != nil {
		t.Fatalf("Pick on mgr2 failed: %v", err)
	}
	if localPath(t, pick2) != localPath(t, c2) {
		t.Errorf("mgr2 first Pick = %q, want %q (cursor did not persist)",
			localPath(t, pick2), localPath(t, c2))
	}
}

// TestRoundRobin_PerPoolCursorIsolation verifies that two pools sharing
// the same StateStore do not trample each other's cursors. Pool "a" and
// pool "b" each get one pick — both should advance from their own zero,
// not share a global counter.
func TestRoundRobin_PerPoolCursorIsolation(t *testing.T) {
	store := newMemStateStore()
	mgr := pool.NewManager()

	aCandidates := []common.Storage{configuredLocal(t), configuredLocal(t)}
	bCandidates := []common.Storage{configuredLocal(t), configuredLocal(t)}

	if err := mgr.Register("a", aCandidates, &pool.RoundRobinStrategy{State: store}); err != nil {
		t.Fatalf("Register a failed: %v", err)
	}
	if err := mgr.Register("b", bCandidates, &pool.RoundRobinStrategy{State: store}); err != nil {
		t.Fatalf("Register b failed: %v", err)
	}

	ctx := context.Background()
	pickA, err := mgr.Pick(ctx, "a", pool.Hint{})
	if err != nil {
		t.Fatalf("Pick a failed: %v", err)
	}
	pickB, err := mgr.Pick(ctx, "b", pool.Hint{})
	if err != nil {
		t.Fatalf("Pick b failed: %v", err)
	}

	if localPath(t, pickA) != localPath(t, aCandidates[0]) {
		t.Errorf("Pool a first pick = %q, want %q", localPath(t, pickA), localPath(t, aCandidates[0]))
	}
	if localPath(t, pickB) != localPath(t, bCandidates[0]) {
		t.Errorf("Pool b first pick = %q, want %q (cursors should be isolated per pool)",
			localPath(t, pickB), localPath(t, bCandidates[0]))
	}
}

// TestRoundRobin_NilStateStore verifies the strategy refuses to operate
// without a StateStore. Cursor persistence is non-negotiable.
func TestRoundRobin_NilStateStore(t *testing.T) {
	mgr := pool.NewManager()
	if err := mgr.Register("p", []common.Storage{configuredLocal(t)}, &pool.RoundRobinStrategy{}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	_, err := mgr.Pick(context.Background(), "p", pool.Hint{})
	var missing *pool.MissingStateStoreError
	if !errors.As(err, &missing) {
		t.Fatalf("expected MissingStateStoreError, got %v", err)
	}
}

// TestLocalOnly_RejectsCloudCandidate registers a pool with one local
// and one fake-cloud (memory) candidate. Through the LocalOnly wrapper,
// any pick that lands on memory must surface NonLocalCandidateError.
//
// We force the pick to land on memory by putting it at index 0 and using
// a fresh round-robin (cursor = 0 → memory).
func TestLocalOnly_RejectsCloudCandidate(t *testing.T) {
	store := newMemStateStore()
	mgr := pool.NewManager()
	candidates := []common.Storage{
		memory.New(),       // index 0 — does not satisfy PathAccessor
		configuredLocal(t), // index 1 — satisfies PathAccessor
	}
	rr := &pool.RoundRobinStrategy{State: store}
	if err := mgr.Register("mixed", candidates, rr); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	wrapped := pool.LocalOnly(mgr)

	// First pick targets index 0 — the memory backend. LocalOnly must
	// reject it.
	_, err := wrapped.Pick(context.Background(), "mixed", pool.Hint{})
	var nlc *pool.NonLocalCandidateError
	if !errors.As(err, &nlc) {
		t.Fatalf("expected NonLocalCandidateError, got %v", err)
	}
	if nlc.Pool != "mixed" {
		t.Errorf("NonLocalCandidateError.Pool = %q, want %q", nlc.Pool, "mixed")
	}

	// Second pick should land on the local backend and succeed.
	picked, err := wrapped.Pick(context.Background(), "mixed", pool.Hint{})
	if err != nil {
		t.Fatalf("second Pick failed: %v", err)
	}
	if _, ok := picked.(common.PathAccessor); !ok {
		t.Errorf("LocalOnly returned non-PathAccessor on second pick: %T", picked)
	}
}

// TestLocalOnly_AcceptsAllLocalCandidates registers a pool with two
// local candidates and verifies that LocalOnly returns each in turn
// without any rejection — the wrapper must be a no-op when the
// underlying pool is already filesystem-only.
func TestLocalOnly_AcceptsAllLocalCandidates(t *testing.T) {
	mgr := pool.NewManager()
	c1 := configuredLocal(t)
	c2 := configuredLocal(t)
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}
	if err := mgr.Register("local-only", []common.Storage{c1, c2}, rr); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	wrapped := pool.LocalOnly(mgr)
	for i := 0; i < 4; i++ {
		picked, err := wrapped.Pick(context.Background(), "local-only", pool.Hint{})
		if err != nil {
			t.Fatalf("Pick #%d failed: %v", i, err)
		}
		if _, ok := picked.(common.PathAccessor); !ok {
			t.Errorf("Pick #%d returned non-PathAccessor: %T", i, picked)
		}
	}
}

// TestLocalOnly_PropagatesInnerErrors verifies that errors from the
// inner Manager (PoolNotFoundError, EmptyPoolError) flow through the
// LocalOnly wrapper unchanged.
func TestLocalOnly_PropagatesInnerErrors(t *testing.T) {
	mgr := pool.NewManager()
	wrapped := pool.LocalOnly(mgr)

	_, err := wrapped.Pick(context.Background(), "ghost", pool.Hint{})
	var pnf *pool.PoolNotFoundError
	if !errors.As(err, &pnf) {
		t.Fatalf("expected PoolNotFoundError to propagate, got %v", err)
	}
}

// TestLocalOnly_ListAndRegisterDelegate verifies that Register and List
// on the wrapper observe the same state as the inner Manager.
func TestLocalOnly_ListAndRegisterDelegate(t *testing.T) {
	mgr := pool.NewManager()
	wrapped := pool.LocalOnly(mgr)
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}

	if err := wrapped.Register("via-wrapper", []common.Storage{configuredLocal(t)}, rr); err != nil {
		t.Fatalf("Register via wrapper failed: %v", err)
	}

	pools, err := mgr.List(context.Background())
	if err != nil {
		t.Fatalf("inner.List failed: %v", err)
	}
	if len(pools) != 1 || pools[0].Name != "via-wrapper" {
		t.Errorf("inner.List = %+v, want [{Name: via-wrapper}]", pools)
	}

	wrappedPools, err := wrapped.List(context.Background())
	if err != nil {
		t.Fatalf("wrapped.List failed: %v", err)
	}
	if len(wrappedPools) != 1 || wrappedPools[0].Name != "via-wrapper" {
		t.Errorf("wrapped.List = %+v, want [{Name: via-wrapper}]", wrappedPools)
	}
}

// TestLeastUtilization_NilStateStore verifies the strategy refuses to
// operate without a StateStore — load tallies cannot be kept without one.
func TestLeastUtilization_NilStateStore(t *testing.T) {
	s := &pool.LeastUtilizationStrategy{}

	_, err := s.Pick(context.Background(), []common.Storage{configuredLocal(t)}, pool.Hint{})
	var missing *pool.MissingStateStoreError
	if !errors.As(err, &missing) {
		t.Fatalf("expected MissingStateStoreError, got %v", err)
	}
	if missing.Strategy != "least_utilization" {
		t.Errorf("Strategy = %q, want least_utilization", missing.Strategy)
	}
}

// TestLeastUtilization_EmptyCandidates verifies that Pick with a nil or
// empty candidate slice returns a typed EmptyPoolError.
func TestLeastUtilization_EmptyCandidates(t *testing.T) {
	ctx := context.Background()
	s := &pool.LeastUtilizationStrategy{State: newMemStateStore()}

	var emptyErr *pool.EmptyPoolError
	if _, err := s.Pick(ctx, nil, pool.Hint{}); !errors.As(err, &emptyErr) {
		t.Fatalf("expected EmptyPoolError for nil candidates, got %v", err)
	}
	if _, err := s.Pick(ctx, []common.Storage{}, pool.Hint{}); !errors.As(err, &emptyErr) {
		t.Fatalf("expected EmptyPoolError for empty slice, got %v", err)
	}
}

// TestLeastUtilization_DeterministicTieBreak verifies that with all
// tallies equal the lowest candidate index wins, and that without size
// hints repeated picks distribute one assignment per candidate in index
// order before wrapping around.
func TestLeastUtilization_DeterministicTieBreak(t *testing.T) {
	mgr := pool.NewManager()
	c1 := configuredLocal(t)
	c2 := configuredLocal(t)
	c3 := configuredLocal(t)
	s := &pool.LeastUtilizationStrategy{State: newMemStateStore()}

	if err := mgr.Register("default", []common.Storage{c1, c2, c3}, s); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	ctx := context.Background()
	wantPaths := []string{
		localPath(t, c1), localPath(t, c2), localPath(t, c3), // first round
		localPath(t, c1), localPath(t, c2), localPath(t, c3), // wrap-around
	}
	for i, want := range wantPaths {
		got, err := mgr.Pick(ctx, "default", pool.Hint{})
		if err != nil {
			t.Fatalf("Pick #%d failed: %v", i, err)
		}
		if localPath(t, got) != want {
			t.Errorf("Pick #%d returned path %q, want %q", i, localPath(t, got), want)
		}
	}
}

// TestLeastUtilization_SizeHintWeighting verifies that SizeHint weights
// the load tallies: after a large placement on one candidate, subsequent
// picks favor the other candidate until its cumulative load catches up.
func TestLeastUtilization_SizeHintWeighting(t *testing.T) {
	mgr := pool.NewManager()
	c1 := configuredLocal(t)
	c2 := configuredLocal(t)
	s := &pool.LeastUtilizationStrategy{State: newMemStateStore()}

	if err := mgr.Register("weighted", []common.Storage{c1, c2}, s); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	ctx := context.Background()

	// First pick (tie) lands on c1 and assigns it 100 units.
	got, err := mgr.Pick(ctx, "weighted", pool.Hint{SizeHint: 100})
	if err != nil {
		t.Fatalf("Pick failed: %v", err)
	}
	if localPath(t, got) != localPath(t, c1) {
		t.Fatalf("first pick = %q, want %q", localPath(t, got), localPath(t, c1))
	}

	// The next four 25-unit picks must all land on c2 — its load stays
	// below c1's 100 before each pick (0, 25, 50, 75).
	for i := 0; i < 4; i++ {
		got, err := mgr.Pick(ctx, "weighted", pool.Hint{SizeHint: 25})
		if err != nil {
			t.Fatalf("Pick #%d failed: %v", i, err)
		}
		if localPath(t, got) != localPath(t, c2) {
			t.Errorf("Pick #%d = %q, want %q (c2 is less utilized)", i, localPath(t, got), localPath(t, c2))
		}
	}

	// Loads are now tied at 100 — the tie breaks back to c1.
	got, err = mgr.Pick(ctx, "weighted", pool.Hint{SizeHint: 1})
	if err != nil {
		t.Fatalf("tie-break Pick failed: %v", err)
	}
	if localPath(t, got) != localPath(t, c1) {
		t.Errorf("tie-break pick = %q, want %q", localPath(t, got), localPath(t, c1))
	}
}

// TestLeastUtilization_TalliesPersist verifies that a new strategy value
// sharing the same StateStore continues from the persisted tallies
// instead of resetting — the property that makes placement survive
// daemon restarts.
func TestLeastUtilization_TalliesPersist(t *testing.T) {
	store := newMemStateStore()
	c1 := configuredLocal(t)
	c2 := configuredLocal(t)
	candidates := []common.Storage{c1, c2}
	ctx := context.Background()

	mgr1 := pool.NewManager()
	if err := mgr1.Register("p", candidates, &pool.LeastUtilizationStrategy{State: store}); err != nil {
		t.Fatalf("Register on mgr1 failed: %v", err)
	}
	if _, err := mgr1.Pick(ctx, "p", pool.Hint{SizeHint: 50}); err != nil {
		t.Fatalf("Pick on mgr1 failed: %v", err)
	}

	// A fresh Manager and strategy with the same StateStore must see
	// c1's 50-unit load and pick c2.
	mgr2 := pool.NewManager()
	if err := mgr2.Register("p", candidates, &pool.LeastUtilizationStrategy{State: store}); err != nil {
		t.Fatalf("Register on mgr2 failed: %v", err)
	}
	got, err := mgr2.Pick(ctx, "p", pool.Hint{})
	if err != nil {
		t.Fatalf("Pick on mgr2 failed: %v", err)
	}
	if localPath(t, got) != localPath(t, c2) {
		t.Errorf("mgr2 pick = %q, want %q (tallies did not persist)", localPath(t, got), localPath(t, c2))
	}
}

// TestLeastUtilization_PerPoolIsolation verifies two pools sharing the
// same StateStore keep independent load tallies.
func TestLeastUtilization_PerPoolIsolation(t *testing.T) {
	store := newMemStateStore()
	mgr := pool.NewManager()
	ctx := context.Background()

	aCandidates := []common.Storage{configuredLocal(t), configuredLocal(t)}
	bCandidates := []common.Storage{configuredLocal(t), configuredLocal(t)}

	if err := mgr.Register("a", aCandidates, &pool.LeastUtilizationStrategy{State: store}); err != nil {
		t.Fatalf("Register a failed: %v", err)
	}
	if err := mgr.Register("b", bCandidates, &pool.LeastUtilizationStrategy{State: store}); err != nil {
		t.Fatalf("Register b failed: %v", err)
	}

	// Load up pool a's first candidate. Pool b's tallies must be
	// unaffected — its first pick still lands on index 0.
	if _, err := mgr.Pick(ctx, "a", pool.Hint{SizeHint: 1000}); err != nil {
		t.Fatalf("Pick a failed: %v", err)
	}
	pickB, err := mgr.Pick(ctx, "b", pool.Hint{})
	if err != nil {
		t.Fatalf("Pick b failed: %v", err)
	}
	if localPath(t, pickB) != localPath(t, bCandidates[0]) {
		t.Errorf("Pool b first pick = %q, want %q (tallies should be isolated per pool)",
			localPath(t, pickB), localPath(t, bCandidates[0]))
	}
}

// TestLeastUtilization_ContextCancelled verifies Pick propagates context
// cancellation instead of producing a pick.
func TestLeastUtilization_ContextCancelled(t *testing.T) {
	mgr := pool.NewManager()
	s := &pool.LeastUtilizationStrategy{State: newMemStateStore()}
	if err := mgr.Register("p", []common.Storage{configuredLocal(t)}, s); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := mgr.Pick(ctx, "p", pool.Hint{}); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// TestLeastUtilization_Name verifies the strategy's canonical name.
func TestLeastUtilization_Name(t *testing.T) {
	s := &pool.LeastUtilizationStrategy{}
	if got := s.Name(); got != "least_utilization" {
		t.Errorf("Name() = %q, want least_utilization", got)
	}
}

// TestManager_List_Empty verifies List on a fresh Manager returns an
// empty slice with no error.
func TestManager_List_Empty(t *testing.T) {
	mgr := pool.NewManager()
	pools, err := mgr.List(context.Background())
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(pools) != 0 {
		t.Errorf("List on empty manager returned %d pools, want 0", len(pools))
	}
}

// TestManager_List_DeterministicOrder verifies the returned slice is
// sorted by name, regardless of registration order.
func TestManager_List_DeterministicOrder(t *testing.T) {
	mgr := pool.NewManager()
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}
	for _, name := range []string{"charlie", "alpha", "bravo"} {
		if err := mgr.Register(name, []common.Storage{configuredLocal(t)}, rr); err != nil {
			t.Fatalf("Register %s failed: %v", name, err)
		}
	}

	pools, err := mgr.List(context.Background())
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(pools) != 3 {
		t.Fatalf("List returned %d pools, want 3", len(pools))
	}
	wantOrder := []string{"alpha", "bravo", "charlie"}
	for i, p := range pools {
		if p.Name != wantOrder[i] {
			t.Errorf("pools[%d].Name = %q, want %q", i, p.Name, wantOrder[i])
		}
	}
}

// TestManager_List_CandidatesAreCopied verifies that mutating the
// returned Candidates slice does not mutate the manager's internal
// state. The defensive copy in List is what makes this safe.
func TestManager_List_CandidatesAreCopied(t *testing.T) {
	mgr := pool.NewManager()
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}
	original := []common.Storage{configuredLocal(t), configuredLocal(t)}
	if err := mgr.Register("p", original, rr); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	pools, err := mgr.List(context.Background())
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	pools[0].Candidates[0] = nil // try to corrupt internal state

	picked, err := mgr.Pick(context.Background(), "p", pool.Hint{})
	if err != nil {
		t.Fatalf("Pick after List mutation failed: %v", err)
	}
	if picked == nil {
		t.Errorf("Pick returned nil; List mutation corrupted internal candidates")
	}
}

// TestManager_Register_DefensiveCopy verifies that mutating the
// candidates slice the caller passed to Register does not mutate the
// manager's internal state.
func TestManager_Register_DefensiveCopy(t *testing.T) {
	mgr := pool.NewManager()
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}
	c1 := configuredLocal(t)
	c2 := configuredLocal(t)
	candidates := []common.Storage{c1, c2}
	if err := mgr.Register("p", candidates, rr); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Mutate the caller's slice — manager must be unaffected.
	candidates[0] = nil

	picked, err := mgr.Pick(context.Background(), "p", pool.Hint{})
	if err != nil {
		t.Fatalf("Pick failed: %v", err)
	}
	if picked == nil {
		t.Errorf("Pick returned nil; caller mutation leaked into manager")
	}
}

// TestRoundRobin_Name verifies the strategy's canonical name.
func TestRoundRobin_Name(t *testing.T) {
	s := &pool.RoundRobinStrategy{State: newMemStateStore()}
	if got := s.Name(); got != "round_robin" {
		t.Errorf("Name() = %q, want round_robin", got)
	}
}

// TestRoundRobinStrategy_EmptyCandidates verifies that calling Pick
// directly with a nil or empty candidate slice returns a typed
// EmptyPoolError instead of panicking with an integer divide-by-zero on
// cursor % len(candidates).
func TestRoundRobinStrategy_EmptyCandidates(t *testing.T) {
	ctx := context.Background()
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}

	picked, err := rr.Pick(ctx, nil, pool.Hint{})
	if picked != nil {
		t.Errorf("expected nil storage, got %v", picked)
	}
	var emptyErr *pool.EmptyPoolError
	if !errors.As(err, &emptyErr) {
		t.Fatalf("expected EmptyPoolError for nil candidates, got %v", err)
	}

	if _, err := rr.Pick(ctx, []common.Storage{}, pool.Hint{}); !errors.As(err, &emptyErr) {
		t.Fatalf("expected EmptyPoolError for empty slice, got %v", err)
	}
}

// TestRoundRobin_ContextCancelled verifies Pick respects a cancelled
// context. It returns context.Canceled — strategies must propagate
// cancellation rather than silently produce a pick.
func TestRoundRobin_ContextCancelled(t *testing.T) {
	mgr := pool.NewManager()
	rr := &pool.RoundRobinStrategy{State: newMemStateStore()}
	if err := mgr.Register("p", []common.Storage{configuredLocal(t)}, rr); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := mgr.Pick(ctx, "p", pool.Hint{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
