package repo

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	enclave "github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-objstore/backup/pkg/chunker"
	"github.com/jeremyhahn/go-objstore/backup/pkg/keyseam"
	"github.com/jeremyhahn/go-objstore/backup/pkg/seal"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

const testPrefix = "backups/site"

// smallChunks keeps test files to a few chunks each.
var smallChunks = chunker.Params{Min: 64, Avg: 256, Max: 1024}

func passphrase(t *testing.T, s string) keyseam.Provider {
	t.Helper()
	h, err := enclave.NewMemory([]byte(s))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	p, err := keyseam.NewPassphraseProvider(h, 0)
	if err != nil {
		t.Fatalf("NewPassphraseProvider: %v", err)
	}
	return p
}

// newRepo initializes a repository in a fresh memory store.
func newRepo(t *testing.T) (*Repository, *store.Memory) {
	t.Helper()
	st := store.NewMemory()
	r, err := Init(context.Background(), st, testPrefix, passphrase(t, "correct horse"),
		Options{Chunker: smallChunks, PackSize: MinPackSize})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, st
}

func assertRepoError(t *testing.T, err error, want ...error) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Op == "" || e.Error() == "" {
		t.Fatalf("error %v is not a *repo.Error", err)
	}
	for _, w := range want {
		if !errors.Is(err, w) {
			t.Fatalf("error %v does not match %v", err, w)
		}
	}
}

// faulty is a store whose calls fail when fail returns an error.
type faulty struct {
	store.Store
	fail func(op, name string) error
}

func (f *faulty) check(op, name string) error {
	if f.fail == nil {
		return nil
	}
	return f.fail(op, name)
}

func (f *faulty) Put(ctx context.Context, name string, data []byte) error {
	if err := f.check("put", name); err != nil {
		return err
	}
	return f.Store.Put(ctx, name, data)
}

func (f *faulty) Get(ctx context.Context, name string) ([]byte, error) {
	if err := f.check("get", name); err != nil {
		return nil, err
	}
	return f.Store.Get(ctx, name)
}

func (f *faulty) List(ctx context.Context, prefix string) ([]string, error) {
	if err := f.check("list", prefix); err != nil {
		return nil, err
	}
	return f.Store.List(ctx, prefix)
}

func (f *faulty) Exists(ctx context.Context, name string) (bool, error) {
	if err := f.check("exists", name); err != nil {
		return false, err
	}
	return f.Store.Exists(ctx, name)
}

var errDown = errors.New("store down")

// failOn fails the given operation on names containing part.
func failOn(op, part string) func(string, string) error {
	return func(o, name string) error {
		if o == op && strings.Contains(name, part) {
			return errDown
		}
		return nil
	}
}

// brokenProvider fails every key operation.
type brokenProvider struct{}

var errHSM = errors.New("hsm unavailable")

func (brokenProvider) WrapKey(context.Context, enclave.ByteAccess) ([]byte, error) {
	return nil, errHSM
}

func (brokenProvider) UnwrapKey(context.Context, []byte) (enclave.ByteAccess, error) {
	return nil, errHSM
}

func TestInitOpen(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	cfg := r.Config()
	if cfg.Version != formatVersion || cfg.PackSize != MinPackSize || cfg.Chunker != smallChunks || cfg.ID == (seal.ID{}) {
		t.Fatalf("config %+v", cfg)
	}
	if r.Layout().Prefix() != testPrefix+"/" {
		t.Fatalf("prefix %q", r.Layout().Prefix())
	}

	_, err := Init(ctx, st, testPrefix, passphrase(t, "correct horse"), Options{})
	assertRepoError(t, err, ErrRepositoryExists)

	o, err := Open(ctx, st, testPrefix, passphrase(t, "correct horse"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !reflect.DeepEqual(o.Config(), cfg) {
		t.Fatalf("opened config %+v, want %+v", o.Config(), cfg)
	}
	a, _ := r.ChunkID([]byte("same"))
	b, _ := o.ChunkID([]byte("same"))
	if a != b {
		t.Fatal("reopened repository derives other chunk IDs")
	}
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err = o.ChunkID([]byte("x"))
	assertRepoError(t, err, ErrKeys)

	// A wrong passphrase fails with the key seam's own typed error.
	_, err = Open(ctx, st, testPrefix, passphrase(t, "wrong horse"))
	assertRepoError(t, err, ErrNoKey, keyseam.ErrUnwrap)
	var ke *keyseam.Error
	if !errors.As(err, &ke) {
		t.Fatalf("error %v has no *keyseam.Error", err)
	}

	_, err = Open(ctx, store.NewMemory(), testPrefix, passphrase(t, "correct horse"))
	assertRepoError(t, err, ErrNoRepository)
}

func TestOpenPassesOverOtherKeys(t *testing.T) {
	ctx := context.Background()
	_, st := newRepo(t)
	// A key object that is not a wrapped key at all, and one from another
	// repository under the same passphrase.
	st.Replace(testPrefix+"/keys/00", []byte("not a key"))
	other, err := Init(ctx, st, "elsewhere", passphrase(t, "correct horse"), Options{})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	_ = other.Close()
	names, _ := st.List(ctx, "elsewhere/keys/")
	wrapped, _ := st.Get(ctx, names[0])
	st.Replace(testPrefix+"/keys/01", wrapped)

	r, err := Open(ctx, st, testPrefix, passphrase(t, "correct horse"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = r.Close()

	// With only the other repository's key, nothing opens the config.
	for _, n := range keyNames(t, st) {
		if !strings.HasSuffix(n, "/01") {
			st.Delete(n)
		}
	}
	_, err = Open(ctx, st, testPrefix, passphrase(t, "correct horse"))
	assertRepoError(t, err, ErrNoKey, seal.ErrOpen)
}

func TestKeySlots(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	if got := r.Config(); got.Version != formatVersion || len(got.Keys) != 1 || got.Keys[0] != r.Slot() {
		t.Fatalf("Init config = %+v, slot %s", got, r.Slot())
	}
	// Data sealed under the data key before any key change.
	data := []byte("snapshot data")
	sealedData, err := r.keys.Seal(seal.TypeConfig, []byte("old"), seal.CompressionNone, data)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	first := r.Slot()

	// Two keys: either opens.
	added, err := r.AddKey(ctx, passphrase(t, "second key"))
	if err != nil {
		t.Fatalf("AddKey: %v", err)
	}
	for _, pass := range []string{"correct horse", "second key"} {
		o, err := Open(ctx, st, testPrefix, passphrase(t, pass))
		if err != nil {
			t.Fatalf("Open %q: %v", pass, err)
		}
		if keys, _ := o.Keys(ctx); len(keys) != 2 || o.Config().Generation != 1 {
			t.Fatalf("keys = %v, generation %d", keys, o.Config().Generation)
		}
		_ = o.Close()
	}

	// Remove the added key: it no longer opens, though its object stays.
	if err := r.RemoveKey(ctx, added); err != nil {
		t.Fatalf("RemoveKey: %v", err)
	}
	if ok, _ := st.Exists(ctx, r.layout.Name(KindKey, added)); !ok {
		t.Fatal("RemoveKey deleted the key object")
	}
	_, err = Open(ctx, st, testPrefix, passphrase(t, "second key"))
	assertRepoError(t, err, ErrNoKey, ErrKeyRemoved)
	assertRepoError(t, r.RemoveKey(ctx, added), ErrUnknownKey)

	// The last key cannot go.
	assertRepoError(t, r.RemoveKey(ctx, first), ErrLastKey)

	// A key change: the new passphrase opens, the old is refused, and the
	// data sealed before still opens.
	newID, err := r.ChangeKey(ctx, passphrase(t, "new horse"))
	if err != nil {
		t.Fatalf("ChangeKey: %v", err)
	}
	if r.Slot() != newID {
		t.Fatalf("slot = %s, want %s", r.Slot(), newID)
	}
	_, err = Open(ctx, st, testPrefix, passphrase(t, "correct horse"))
	assertRepoError(t, err, ErrNoKey, ErrKeyRemoved)
	o, err := Open(ctx, st, testPrefix, passphrase(t, "new horse"))
	if err != nil {
		t.Fatalf("Open new: %v", err)
	}
	defer func() { _ = o.Close() }()
	got, err := o.keys.Open(seal.TypeConfig, []byte("old"), sealedData)
	if err != nil || string(got) != string(data) {
		t.Fatalf("Get = %q, %v", got, err)
	}
	if keys, _ := o.Keys(ctx); len(keys) != 1 || keys[0] != newID {
		t.Fatalf("keys = %v", keys)
	}

	// A stale handle writing a generation that exists is a conflict.
	r.cfg.Generation = 0
	_, err = r.AddKey(ctx, passphrase(t, "late"))
	assertRepoError(t, err, ErrKeyConflict)
}

func TestFormat1Upgrade(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	// Rewrite the config as the format before key slots wrote it.
	v1 := r.Config()
	v1.Version = formatVersionV1
	v1.Keys = nil
	plain, _ := json.Marshal(v1)
	sealed, err := r.keys.Seal(seal.TypeConfig, configID, seal.CompressionNone, plain)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	st.Replace(testPrefix+"/config", sealed)

	o, err := Open(ctx, st, testPrefix, passphrase(t, "correct horse"))
	if err != nil {
		t.Fatalf("Open format 1: %v", err)
	}
	if o.Config().Version != formatVersionV1 {
		t.Fatalf("version = %d", o.Config().Version)
	}
	keys, err := o.Keys(ctx)
	if err != nil || len(keys) != 1 || keys[0] != o.Slot() {
		t.Fatalf("Keys = %v, %v", keys, err)
	}
	if _, err := o.AddKey(ctx, passphrase(t, "second key")); err != nil {
		t.Fatalf("AddKey: %v", err)
	}
	_ = o.Close()

	// The first key change upgraded the repository to format 2.
	u, err := Open(ctx, st, testPrefix, passphrase(t, "second key"))
	if err != nil {
		t.Fatalf("Open upgraded: %v", err)
	}
	defer func() { _ = u.Close() }()
	if c := u.Config(); c.Version != formatVersion || c.Generation != 1 || len(c.Keys) != 2 {
		t.Fatalf("upgraded config = %+v", c)
	}

	// A generation whose name does not match its content is corrupt.
	gen, _ := st.Get(ctx, testPrefix+"/keyring/0000000000000001")
	st.Replace(testPrefix+"/keyring/0000000000000002", gen)
	_, err = Open(ctx, st, testPrefix, passphrase(t, "second key"))
	assertRepoError(t, err, ErrCorrupt)
}

func keyNames(t *testing.T, st store.Store) []string {
	t.Helper()
	names, err := st.List(context.Background(), testPrefix+"/keys/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	return names
}

func TestOpenBadConfig(t *testing.T) {
	ctx := context.Background()
	r, st := newRepo(t)
	sealCfg := func(v any) []byte {
		plain, _ := json.Marshal(v)
		sealed, err := r.keys.Seal(seal.TypeConfig, configID, seal.CompressionNone, plain)
		if err != nil {
			t.Fatalf("Seal: %v", err)
		}
		return sealed
	}
	bad := r.Config()
	bad.Version = formatVersion + 1
	small := r.Config()
	small.PackSize = 1
	for name, data := range map[string][]byte{
		"version":   sealCfg(bad),
		"pack size": sealCfg(small),
		"not json":  sealCfg("text"),
	} {
		t.Run(name, func(t *testing.T) {
			st.Replace(testPrefix+"/config", data)
			_, err := Open(ctx, st, testPrefix, passphrase(t, "correct horse"))
			assertRepoError(t, err, ErrCorrupt)
		})
	}
}

func TestInitOptions(t *testing.T) {
	ctx := context.Background()
	for name, opts := range map[string]Options{
		"pack size": {PackSize: MinPackSize - 1},
		"too big":   {PackSize: MaxPackSize + 1},
		"chunker":   {Chunker: chunker.Params{Min: 1, Avg: 3, Max: 2}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Init(ctx, store.NewMemory(), "", brokenProvider{}, opts)
			assertRepoError(t, err, ErrInvalidOptions)
		})
	}
	_, err := Init(ctx, store.NewMemory(), "", brokenProvider{}, Options{})
	assertRepoError(t, err, ErrKeys, errHSM)
}

func TestStoreFailures(t *testing.T) {
	ctx := context.Background()
	_, good := newRepo(t)
	for name, tc := range map[string]struct {
		fail func(string, string) error
		open bool
	}{
		"init exists":  {fail: failOn("exists", "config")},
		"init key":     {fail: failOn("put", "keys/")},
		"init config":  {fail: failOn("put", "config")},
		"open config":  {fail: failOn("get", "config"), open: true},
		"open list":    {fail: failOn("list", "/keys/"), open: true},
		"open get key": {fail: failOn("get", "/keys/"), open: true},
	} {
		t.Run(name, func(t *testing.T) {
			var err error
			if tc.open {
				_, err = Open(ctx, &faulty{Store: good, fail: tc.fail}, testPrefix, passphrase(t, "correct horse"))
			} else {
				_, err = Init(ctx, &faulty{Store: store.NewMemory(), fail: tc.fail}, "", passphrase(t, "p"), Options{})
			}
			assertRepoError(t, err, ErrStorage, errDown)
		})
	}

	// A provider that fails for another reason than a wrong key stops Open.
	_, err := Open(ctx, good, testPrefix, brokenProvider{})
	assertRepoError(t, err, errHSM)
}

func TestLayout(t *testing.T) {
	for in, want := range map[string]string{"": "", "a": "a/", "a/b//": "a/b/"} {
		if got := NewLayout(in).Prefix(); got != want {
			t.Fatalf("NewLayout(%q).Prefix() = %q, want %q", in, got, want)
		}
	}
	l := NewLayout("r")
	id := seal.ID{0xab, 0xcd}
	hexID := id.String()
	for k, want := range map[Kind]string{
		KindKey:      "r/keys/" + hexID,
		KindPack:     "r/packs/ab/" + hexID,
		KindIndex:    "r/index/" + hexID,
		KindSnapshot: "r/snapshots/" + hexID,
		KindMark:     "r/marks/" + hexID,
	} {
		name := l.Name(k, id)
		if name != want {
			t.Fatalf("Name(%d) = %q, want %q", k, name, want)
		}
		got, err := l.Parse(k, name)
		if err != nil || got != id {
			t.Fatalf("Parse(%q) = %v, %v", name, got, err)
		}
	}
	if l.Config() != "r/config" || l.Dir(Kind(0)) != "r/unknown/" {
		t.Fatalf("Config %q, Dir(0) %q", l.Config(), l.Dir(Kind(0)))
	}
	for _, tc := range []struct {
		k    Kind
		name string
	}{
		{KindIndex, "r/snapshots/" + hexID},
		{KindIndex, "r/index/" + hexID[:10]},
		{KindIndex, "r/index/" + strings.ToUpper(hexID)},
		{KindPack, "r/packs/" + hexID},
		{KindPack, "r/packs/cd/" + hexID},
		{KindPack, "r/packs/abc/" + hexID},
		{KindPack, "r/packs/ab/zz"},
	} {
		_, err := l.Parse(tc.k, tc.name)
		assertRepoError(t, err, ErrMalformedName)
	}
}
