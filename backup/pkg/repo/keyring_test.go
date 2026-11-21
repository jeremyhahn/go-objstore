package repo

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

// racyStore mimics store.SDK: Put checks Exists and then writes
// unconditionally, so a Put never refuses a name another writer took. It
// also holds the first hold key generation writes at a gate until all of
// them arrive, so their writers have all read the same generation first.
type racyStore struct {
	*store.Memory
	mu   sync.Mutex
	hold int
	gate chan struct{}
}

func newRacyStore(m *store.Memory, hold int) *racyStore {
	return &racyStore{Memory: m, hold: hold, gate: make(chan struct{})}
}

func (s *racyStore) Put(ctx context.Context, name string, data []byte) error {
	if strings.Contains(name, "/"+keyringDir) {
		s.mu.Lock()
		held := s.hold > 0
		if held {
			s.hold--
			if s.hold == 0 {
				close(s.gate)
			}
		}
		s.mu.Unlock()
		if held {
			select {
			case <-s.gate:
			case <-time.After(10 * time.Second):
				return errors.New("racy store: gate timeout")
			}
		}
	}
	ok, err := s.Exists(ctx, name)
	if err != nil {
		return err
	}
	if ok {
		s.Replace(name, data)
		return nil
	}
	return s.Memory.Put(ctx, name, data)
}

// openRacy opens n handles with pass on a racy store over m that gates the
// next n key generation writes.
func openRacy(t *testing.T, m *store.Memory, n int, pass ...string) []*Repository {
	t.Helper()
	st := newRacyStore(m, n)
	hs := make([]*Repository, n)
	for i := range hs {
		h, err := Open(context.Background(), st, testPrefix, passphrase(t, pass[i]))
		if err != nil {
			t.Fatalf("Open %d: %v", i, err)
		}
		t.Cleanup(func() { _ = h.Close() })
		hs[i] = h
	}
	return hs
}

// race runs fns concurrently and returns their errors.
func race(fns ...func() error) []error {
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn()
		}()
	}
	wg.Wait()
	return errs
}

func mustOpen(t *testing.T, st store.Store, pass string) {
	t.Helper()
	o, err := Open(context.Background(), st, testPrefix, passphrase(t, pass))
	if err != nil {
		t.Fatalf("Open %q: %v", pass, err)
	}
	_ = o.Close()
}

func TestKeyChangeRaceKeepsRemoval(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	a, err := r.AddKey(ctx, passphrase(t, "key a"))
	if err != nil {
		t.Fatalf("AddKey a: %v", err)
	}
	hs := openRacy(t, st, 2, "correct horse", "correct horse")
	errs := race(
		func() error { return hs[0].RemoveKey(ctx, a) },
		func() error { _, err := hs[1].AddKey(ctx, passphrase(t, "key c")); return err },
	)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("change %d: %v", i, err)
		}
	}
	_, err = Open(ctx, st, testPrefix, passphrase(t, "key a"))
	assertRepoError(t, err, ErrNoKey, ErrKeyRemoved)
	mustOpen(t, st, "correct horse")
	mustOpen(t, st, "key c")
}

func TestKeyChangeRacePasswd(t *testing.T) {
	ctx := context.Background()
	_, st := newRepo(t)
	hs := openRacy(t, st, 2, "correct horse", "correct horse")
	errs := race(
		func() error { _, err := hs[0].ChangeKey(ctx, passphrase(t, "new one")); return err },
		func() error { _, err := hs[1].ChangeKey(ctx, passphrase(t, "new two")); return err },
	)
	for i, err := range errs {
		if err != nil {
			t.Fatalf("ChangeKey %d: %v", i, err)
		}
	}
	_, err := Open(ctx, st, testPrefix, passphrase(t, "correct horse"))
	assertRepoError(t, err, ErrNoKey, ErrKeyRemoved)
	mustOpen(t, st, "new one")
	mustOpen(t, st, "new two")
}

func TestKeyChangeRaceEmptySet(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	first := r.Slot()
	second, err := r.AddKey(ctx, passphrase(t, "second key"))
	if err != nil {
		t.Fatalf("AddKey: %v", err)
	}
	hs := openRacy(t, st, 2, "correct horse", "second key")
	errs := race(
		func() error { return hs[0].RemoveKey(ctx, second) },
		func() error { return hs[1].RemoveKey(ctx, first) },
	)
	failed := 0
	for _, err := range errs {
		if err != nil {
			failed++
			assertRepoError(t, err, ErrKeyConflict, ErrNoKey)
		}
	}
	if failed == 0 {
		t.Fatal("both removals of the last two slots succeeded")
	}
	for _, pass := range []string{"correct horse", "second key"} {
		_, err := Open(ctx, st, testPrefix, passphrase(t, pass))
		assertRepoError(t, err, ErrKeyConflict, ErrNoKey)
	}
}

func TestKeyringNames(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	if _, err := r.AddKey(ctx, passphrase(t, "second key")); err != nil {
		t.Fatalf("AddKey: %v", err)
	}
	var gen string
	var data []byte
	st.Each(func(name string, b []byte) {
		if strings.Contains(name, "/"+keyringDir) {
			gen, data = name, b
		}
	})
	if n, ok := r.parseKeyring(gen); !ok || n != 1 {
		t.Fatalf("generation name %q parses to %d, %v", gen, n, ok)
	}
	for _, bad := range []string{
		"x", testPrefix + "/keyring/0000000000000001",
		testPrefix + "/keyring/000000000000000g-" + strings.Repeat("0", 32),
		testPrefix + "/keyring/0000000000000001-" + strings.Repeat("A", 32),
		testPrefix + "/keyring/0000000000000001-" + strings.Repeat("z", 32),
	} {
		if _, ok := r.parseKeyring(bad); ok {
			t.Fatalf("parseKeyring(%q) accepted", bad)
		}
	}
	// A generation copied under another well-formed name is corrupt: the
	// sealed content binds its name.
	for _, other := range []string{
		r.keyringName(1, strings.Repeat("a", 32)),
		r.keyringName(2, strings.Repeat("a", 32)),
	} {
		st.Replace(other, data)
		_, err := Open(ctx, st, testPrefix, passphrase(t, "second key"))
		assertRepoError(t, err, ErrCorrupt)
		st.Delete(other)
	}
	// A generation without a delta is corrupt.
	g := generation{Config: r.Config()}
	g.Generation = 2
	name := r.keyringName(2, strings.Repeat("b", 32))
	plain, _ := json.Marshal(g)
	sealed, err := r.keys.Seal(seal.TypeConfig, configSealID(r.layout, name), seal.CompressionNone, plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	st.Replace(name, sealed)
	_, err = Open(ctx, st, testPrefix, passphrase(t, "second key"))
	assertRepoError(t, err, ErrCorrupt)
	st.Delete(name)
	mustOpen(t, st, "second key")
}
