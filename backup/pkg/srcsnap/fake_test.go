package srcsnap

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// toolDir is where the fake LookPath puts every tool.
const toolDir = "/fake/sbin"

// fakeRunner records each argument vector, with the tool's directory
// stripped, and answers from scripted outputs and failures matched by
// prefix.
type fakeRunner struct {
	mu    sync.Mutex
	calls []string
	out   map[string]string
	fail  map[string]error
	hook  func(line string)
}

func (r *fakeRunner) Run(_ context.Context, argv []string) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !filepath.IsAbs(argv[0]) {
		return nil, errors.New("relative tool path")
	}
	line := strings.Join(append([]string{strings.TrimPrefix(argv[0], toolDir+"/")}, argv[1:]...), " ")
	r.calls = append(r.calls, line)
	if r.hook != nil {
		r.hook(line)
	}
	for k, err := range r.fail {
		if strings.HasPrefix(line, k) {
			return nil, err
		}
	}
	for k, o := range r.out {
		if strings.HasPrefix(line, k) {
			return []byte(o), nil
		}
	}
	return nil, nil
}

func (r *fakeRunner) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.calls
	r.calls = nil
	return c
}

// fakeLocator places each path on the volume whose key is its longest
// prefix and reports the path as resolved unchanged.
type fakeLocator struct {
	vols   map[string]Volume
	mounts map[string]bool
	err    error
}

func (l fakeLocator) Locate(path string) (Volume, string, error) {
	if l.err != nil {
		return Volume{}, "", l.err
	}
	best := ""
	for k := range l.vols {
		if within(k, path) && len(k) > len(best) {
			best = k
		}
	}
	if best == "" {
		return Volume{}, "", errors.New("no volume")
	}
	return l.vols[best], path, nil
}

func (l fakeLocator) MountPoints() (map[string]bool, error) {
	if l.err != nil {
		return nil, l.err
	}
	return l.mounts, nil
}

// seqRand yields 0, 1, 2, ... so the names are predictable.
type seqRand struct{ n byte }

func (r *seqRand) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = r.n
		r.n++
	}
	return len(b), nil
}

const (
	name1 = NamePrefix + "0001020304050607"
	name2 = NamePrefix + "08090a0b0c0d0e0f"
)

type rig struct {
	s   *Snapshotter
	r   *fakeRunner
	log *bytes.Buffer
	dir string
}

func newRig(t *testing.T, cfg Config, loc Locator) *rig {
	t.Helper()
	if cfg.Dir == "" {
		cfg.Dir = t.TempDir()
	}
	r := &fakeRunner{out: map[string]string{}, fail: map[string]error{}}
	log := &bytes.Buffer{}
	s, err := New(cfg, Env{
		Runner:   r,
		Locator:  loc,
		LookPath: func(tool string) (string, error) { return toolDir + "/" + tool, nil },
		Log:      slog.New(slog.NewTextHandler(log, nil)),
		Rand:     &seqRand{},
		HostDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &rig{s: s, r: r, log: log, dir: s.Config().Dir}
}

func wantCalls(t *testing.T, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("calls:\n got %q\nwant %q", got, want)
	}
}

func wantErr(t *testing.T, err error, sentinel error) {
	t.Helper()
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("err = %T, want *Error", err)
	}
}

// entries lists the snapshot directory without the lock file.
func entries(t *testing.T, dir string) []string {
	t.Helper()
	es, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range es {
		if e.Name() != lockName {
			names = append(names, e.Name())
		}
	}
	return names
}

// unlocked checks that the snapshot directory's lock is free.
func unlocked(t *testing.T, s *Snapshotter) {
	t.Helper()
	unlock, err := s.lock()
	if err != nil {
		t.Fatalf("lock still held: %v", err)
	}
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
}
