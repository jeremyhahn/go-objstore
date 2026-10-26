package store

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	objstore "github.com/jeremyhahn/go-objstore/api/sdks/go"
)

// fakeClient is an objstore.Client over a map, paging List two names at a
// time. Methods the store does not call panic through the nil embedded
// interface.
type fakeClient struct {
	objstore.Client
	mu      sync.Mutex
	objects map[string][]byte
	err     error // returned by every call when set
	stream  bool  // Get returns a Reader instead of Data
	nilRes  bool  // Get and List return a nil result
	puts    int
}

func newFakeClient() *fakeClient {
	return &fakeClient{objects: map[string][]byte{}}
}

func (f *fakeClient) Put(_ context.Context, key string, data []byte, _ *objstore.Metadata) (*objstore.PutResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.puts++
	f.objects[key] = append([]byte{}, data...)
	return &objstore.PutResult{Success: true}, nil
}

func (f *fakeClient) Get(_ context.Context, key string) (*objstore.GetResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case f.err != nil:
		return nil, f.err
	case f.nilRes:
		return nil, nil
	}
	data, ok := f.objects[key]
	if !ok {
		return nil, objstore.ErrObjectNotFound
	}
	if f.stream {
		return &objstore.GetResult{Reader: io.NopCloser(bytes.NewReader(data))}, nil
	}
	return &objstore.GetResult{Data: data}, nil
}

func (f *fakeClient) List(_ context.Context, opts *objstore.ListOptions) (*objstore.ListResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case f.err != nil:
		return nil, f.err
	case f.nilRes:
		return nil, nil
	}
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, opts.Prefix) && k > opts.ContinueFrom {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	res := &objstore.ListResult{}
	if len(keys) > 2 {
		keys = keys[:2]
		res.Truncated = true
		res.NextToken = keys[1]
	}
	for _, k := range keys {
		res.Objects = append(res.Objects, &objstore.ObjectInfo{Key: k})
	}
	res.Objects = append(res.Objects, nil)
	return res, nil
}

func (f *fakeClient) Exists(_ context.Context, key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return false, f.err
	}
	_, ok := f.objects[key]
	return ok, nil
}

func (f *fakeClient) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	if _, ok := f.objects[key]; !ok {
		return objstore.ErrObjectNotFound
	}
	delete(f.objects, key)
	return nil
}

// removeContract checks Remove on s, which contract has filled.
func removeContract(t *testing.T, s PruneStore) {
	t.Helper()
	ctx := context.Background()
	if err := s.Remove(ctx, "p/b"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if ok, err := s.Exists(ctx, "p/b"); err != nil || ok {
		t.Fatalf("Exists after Remove = %v, %v", ok, err)
	}
	assertStoreError(t, s.Remove(ctx, "p/b"), ErrNotFound)
	assertStoreError(t, s.Remove(ctx, ""), ErrInvalidName)
	assertStoreError(t, s.Remove(canceled(), "p/c"), context.Canceled)
	// A removed name can be written again.
	if err := s.Put(ctx, "p/b", []byte("data p/b")); err != nil {
		t.Fatalf("Put after Remove: %v", err)
	}
}

func assertStoreError(t *testing.T, err, want error) {
	t.Helper()
	var e *Error
	if !errors.Is(err, want) || !errors.As(err, &e) || e.Op == "" {
		t.Fatalf("error %v is not an *Error matching %v", err, want)
	}
	if e.Error() == "" {
		t.Fatal("empty error text")
	}
}

// contract runs the behaviour every Store must have.
func contract(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()
	for _, name := range []string{"p/b", "p/a", "q/c", "p/c"} {
		if err := s.Put(ctx, name, []byte("data "+name)); err != nil {
			t.Fatalf("Put(%s): %v", name, err)
		}
	}
	// Put once: the second put fails and leaves the object as it was.
	assertStoreError(t, s.Put(ctx, "p/a", []byte("overwrite")), ErrExists)
	got, err := s.Get(ctx, "p/a")
	if err != nil || string(got) != "data p/a" {
		t.Fatalf("Get(p/a) = %q, %v", got, err)
	}
	_, err = s.Get(ctx, "p/missing")
	assertStoreError(t, err, ErrNotFound)

	names, err := s.List(ctx, "p/")
	if err != nil || !slices.Equal(names, []string{"p/a", "p/b", "p/c"}) {
		t.Fatalf("List(p/) = %v, %v", names, err)
	}
	if names, _ := s.List(ctx, "none/"); len(names) != 0 {
		t.Fatalf("List(none/) = %v", names)
	}
	for name, want := range map[string]bool{"q/c": true, "q/d": false} {
		if ok, err := s.Exists(ctx, name); err != nil || ok != want {
			t.Fatalf("Exists(%s) = %v, %v", name, ok, err)
		}
	}

	assertStoreError(t, s.Put(ctx, "", nil), ErrInvalidName)
	_, err = s.Get(ctx, "")
	assertStoreError(t, err, ErrInvalidName)
	_, err = s.Exists(ctx, "")
	assertStoreError(t, err, ErrInvalidName)

	done, cancel := context.WithCancel(ctx)
	cancel()
	assertStoreError(t, s.Put(done, "p/z", nil), context.Canceled)
	_, err = s.Get(done, "p/a")
	assertStoreError(t, err, context.Canceled)
	_, err = s.Exists(done, "p/a")
	assertStoreError(t, err, context.Canceled)
}

func TestMemory(t *testing.T) {
	m := NewMemory()
	contract(t, m)
	removeContract(t, m)
	_, err := m.List(canceled(), "p/")
	assertStoreError(t, err, context.Canceled)

	// Stored bytes are copies.
	data := []byte("mine")
	if err := m.Put(context.Background(), "copy", data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	data[0] = 'X'
	got, _ := m.Get(context.Background(), "copy")
	got[1] = 'X'
	if again, _ := m.Get(context.Background(), "copy"); string(again) != "mine" {
		t.Fatalf("stored object changed to %q", again)
	}

	m.Replace("copy", []byte("theirs"))
	m.Delete("p/a")
	var seen []string
	m.Each(func(name string, data []byte) { seen = append(seen, name+"="+string(data)) })
	if !slices.Equal(seen, []string{"copy=theirs", "p/b=data p/b", "p/c=data p/c", "q/c=data q/c"}) {
		t.Fatalf("Each saw %v", seen)
	}
}

func canceled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestSDK(t *testing.T) {
	f := newFakeClient()
	s, err := NewSDK(f)
	if err != nil {
		t.Fatalf("NewSDK: %v", err)
	}
	contract(t, s)
	if f.puts != 4 {
		t.Fatalf("%d puts reached the server, want 4: an existing name was overwritten", f.puts)
	}
	removeContract(t, s)

	f.stream = true
	if got, err := s.Get(context.Background(), "q/c"); err != nil || string(got) != "data q/c" {
		t.Fatalf("streamed Get = %q, %v", got, err)
	}
	f.stream = false

	// More names than one page.
	for _, n := range []string{"p/d", "p/e"} {
		if err := s.Put(context.Background(), n, nil); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if names, err := s.List(context.Background(), "p/"); err != nil || len(names) != 5 {
		t.Fatalf("paged List = %v, %v", names, err)
	}

	f.nilRes = true
	_, err = s.Get(context.Background(), "p/a")
	assertStoreError(t, err, ErrBackend)
	_, err = s.List(context.Background(), "p/")
	assertStoreError(t, err, ErrBackend)
	f.nilRes = false

	errDown := errors.New("connection refused")
	f.err = errDown
	ctx := context.Background()
	assertStoreError(t, s.Put(ctx, "x", nil), errDown)
	_, err = s.Get(ctx, "x")
	assertStoreError(t, err, ErrBackend)
	_, err = s.List(ctx, "")
	assertStoreError(t, err, errDown)
	_, err = s.Exists(ctx, "x")
	assertStoreError(t, err, ErrBackend)
	assertStoreError(t, s.Remove(ctx, "x"), errDown)

	// The server's own conflict answer, as from a WORM bucket.
	f.err = nil
	conflict := &conflictClient{fakeClient: f}
	cs, _ := NewSDK(conflict)
	assertStoreError(t, cs.Put(ctx, "new", nil), ErrExists)

	if _, err := NewSDK(nil); !errors.Is(err, ErrBackend) {
		t.Fatalf("NewSDK(nil) = %v", err)
	}
}

// conflictClient answers every Put with the SDK's 409 sentinel.
type conflictClient struct{ *fakeClient }

func (c *conflictClient) Put(context.Context, string, []byte, *objstore.Metadata) (*objstore.PutResult, error) {
	return nil, objstore.ErrAlreadyExists
}

func TestSDKStreamReadError(t *testing.T) {
	s, _ := NewSDK(&brokenStream{})
	_, err := s.Get(context.Background(), "x")
	assertStoreError(t, err, ErrBackend)
}

// brokenStream returns a Reader that fails partway.
type brokenStream struct{ objstore.Client }

func (brokenStream) Get(context.Context, string) (*objstore.GetResult, error) {
	r := io.MultiReader(strings.NewReader("part"), errReader{})
	return &objstore.GetResult{Reader: io.NopCloser(r)}, nil
}

type errReader struct{}

var errTruncated = errors.New("stream truncated")

func (errReader) Read([]byte) (int, error) { return 0, errTruncated }
