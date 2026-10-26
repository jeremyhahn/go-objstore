package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	objstore "github.com/jeremyhahn/go-objstore/api/sdks/go"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

const token = "bearer-token-0123456789"

// stepClock is a clock that moves only when the loop sleeps on it: each
// After advances it by the wait at once.
type stepClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *stepClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	ch := make(chan time.Time, 1)
	ch <- c.now
	return ch
}

// noRemove is the backup identity's view of the store: it fails the test
// if anything is removed through it.
type noRemove struct {
	*store.Memory
	t *testing.T
}

func (s noRemove) Remove(context.Context, string) error {
	s.t.Error("the backup identity removed an object")
	return errors.New("permission denied")
}

// countRemove is the prune identity's view of the store.
type countRemove struct {
	*store.Memory
	n *int
}

func (s countRemove) Remove(ctx context.Context, name string) error {
	*s.n++
	return s.Memory.Remove(ctx, name)
}

// serviceRun runs the service with config until stop returns true after a
// run, and returns the exit code and every log record.
func (h *harness) serviceRun(config string, stop func(run int) bool, removes *int) (int, []map[string]any) {
	h.t.Helper()
	h.secrets = append(h.secrets, token)
	path := h.file("service.json", config, 0o600)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stderr, stdout bytes.Buffer
	runs := 0
	e := &env{
		stdin: strings.NewReader(""), stdout: &stdout, stderr: &stderr,
		open: func(ctx context.Context, cfg *objstore.ClientConfig) (store.PruneStore, func() error, error) {
			_, closer, err := h.open(ctx, cfg)
			if err != nil {
				return nil, nil, err
			}
			if strings.HasPrefix(cfg.Address, "prune.") {
				return countRemove{h.st, removes}, closer, nil
			}
			return noRemove{h.st, h.t}, closer, nil
		},
		clock: &stepClock{now: time.Date(2026, 3, 10, 9, 0, 0, 0, time.UTC)},
		snap:  h.snap,
		ran: func() {
			runs++
			if stop(runs) {
				cancel()
			}
		},
	}
	code := runWith(ctx, e, []string{"service", "--config", path})
	for _, s := range h.secrets {
		if strings.Contains(stderr.String(), s) || strings.Contains(stdout.String(), s) {
			h.t.Fatalf("a secret reached the output:\n%s", stderr.String())
		}
	}
	var logs []map[string]any
	for line := range strings.Lines(stderr.String()) {
		if !strings.HasPrefix(line, "{") {
			continue // a command error, not a log record
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			h.t.Fatalf("log line %q: %v", line, err)
		}
		logs = append(logs, rec)
	}
	return code, logs
}

func pick(logs []map[string]any, msg string) []map[string]any {
	var out []map[string]any
	for _, r := range logs {
		if r["msg"] == msg {
			out = append(out, r)
		}
	}
	return out
}

func (h *harness) serviceConfig(src, extra string) string {
	tok := h.file("token", token+"\n", 0o600)
	return fmt.Sprintf(`{
  "connection": {"address": "backup.example:9000", "token_file": %q},
  "repo": "hosts/a", "passphrase_file": %q, "sources": [%q], "host": "web1",
  "schedule": {"every": "1h"}, "log_format": "json"%s
}`, tok, h.pass, src, extra)
}

func TestServiceRuns(t *testing.T) {
	h := newHarness(t)
	h.must(exitOK, h.repo("init")...)
	src := h.source(map[string]string{"a.txt": strings.Repeat("alpha ", 5000), "d/b.txt": "bravo"})
	removes := 0
	cfg := h.serviceConfig(src, `,
  "prune": {"connection": {"address": "prune.example:9000", "token_file": "`+filepath.ToSlash(filepath.Join(h.dir, "token"))+`"}, "keep_last": 1}`)
	code, logs := h.serviceRun(cfg, func(run int) bool {
		if run == 1 {
			if err := os.WriteFile(filepath.Join(src, "c.txt"), []byte("charlie"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return run == 2
	}, &removes)
	if code != exitOK {
		t.Fatalf("service = %d\nlogs: %v", code, logs)
	}
	saved := pick(logs, "snapshot saved")
	if len(saved) != 2 || saved[0]["snapshot"] == saved[1]["snapshot"] {
		t.Fatalf("snapshots saved = %v", saved)
	}
	if saved[0]["files"] != 2.0 || saved[1]["files"] != 3.0 || saved[1]["deduplicated_chunks"].(float64) < 1 ||
		saved[1]["new_bytes"] != float64(len("charlie")) || saved[0]["source"] != src {
		t.Errorf("the second run is not incremental: %v", saved)
	}
	for _, msg := range []string{"service started", "service stopped"} {
		if len(pick(logs, msg)) != 1 {
			t.Errorf("%q logged %d times", msg, len(pick(logs, msg)))
		}
	}
	if n := len(pick(logs, "run finished")); n != 2 || len(pick(logs, "run started")) != 2 {
		t.Errorf("%d runs finished", n)
	}
	pruned := pick(logs, "prune finished")
	if len(pruned) != 2 || pruned[1]["forgotten"] != 1.0 || pruned[1]["kept"] != 1.0 {
		t.Fatalf("prune = %v", pruned)
	}
	if groups := pick(logs, "prune group"); len(groups) != 2 || groups[1]["root"] != src || groups[1]["host"] == "" ||
		groups[1]["kept"] != 1.0 || groups[1]["forgotten"] != 1.0 {
		t.Fatalf("prune groups = %v", groups)
	}
	if removes == 0 {
		t.Error("prune removed nothing through the prune identity")
	}
	// Each run connected as the backup identity, then as the prune
	// identity, and the prune identity used its own token.
	var addrs []string
	for _, c := range h.configs[1:] {
		addrs = append(addrs, c.Address)
		if c.Token != token {
			t.Errorf("connection to %s without the token", c.Address)
		}
	}
	if want := "backup.example:9000 prune.example:9000 backup.example:9000 prune.example:9000"; strings.Join(addrs, " ") != want {
		t.Errorf("connections = %v, want %s", addrs, want)
	}
	out := h.must(exitOK, h.repo("snapshots", "--json")...)
	if !strings.Contains(out, saved[1]["snapshot"].(string)) || strings.Contains(out, saved[0]["snapshot"].(string)) {
		t.Errorf("snapshots after prune: %s", out)
	}
}

func TestServiceFailures(t *testing.T) {
	h := newHarness(t)
	h.must(exitOK, h.repo("init")...)
	src := h.source(map[string]string{"a.txt": "alpha"})

	// A missing source and a wrong passphrase fail their runs, logged, and
	// the service goes on to the next run.
	cfg := h.serviceConfig(src, "")
	cfg = strings.Replace(cfg, fmt.Sprintf("[%q]", src), fmt.Sprintf("[%q, %q]", filepath.Join(src, "missing"), src), 1)
	code, logs := h.serviceRun(cfg, func(run int) bool {
		if run == 1 {
			if err := os.WriteFile(h.pass, []byte("wrong passphrase\n"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return run == 2
	}, nil)
	if code != exitOK {
		t.Fatalf("service = %d", code)
	}
	failed := pick(logs, "run failed")
	if len(failed) != 2 || len(pick(logs, "backup failed")) != 1 || failed[0]["snapshots"] != 1.0 || failed[1]["snapshots"] != 0.0 {
		t.Fatalf("failed runs = %v", failed)
	}
	if err := os.WriteFile(h.pass, []byte(passphrase+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// A failed backup skips the prune.
	removes := 0
	cfg = strings.Replace(h.serviceConfig(filepath.Join(src, "missing"), `, "prune": {"connection": {"address": "prune.example:9000"}, "keep_last": 1}`), `"every": "1h"`, `"daily": "03:00"`, 1)
	h.configs = nil
	if code, logs = h.serviceRun(cfg, func(int) bool { return true }, &removes); code != exitOK || len(pick(logs, "prune finished")) != 0 {
		t.Fatalf("service = %d, logs %v", code, logs)
	}
	for _, c := range h.configs {
		if strings.HasPrefix(c.Address, "prune.") {
			t.Error("prune ran after a failed backup")
		}
	}

	// The connection fails.
	h.openErr = errors.New("connection refused")
	if code, logs = h.serviceRun(h.serviceConfig(src, ""), func(int) bool { return true }, nil); code != exitOK ||
		len(pick(logs, "run failed")) != 1 {
		t.Fatalf("service = %d, logs %v", code, logs)
	}
	h.openErr = nil

	// The prune identity's token file is refused.
	bad := h.file("bad-token", token, 0o644)
	cfg = h.serviceConfig(src, `, "prune": {"connection": {"address": "prune.example:9000", "token_file": "`+filepath.ToSlash(bad)+`"}, "keep_last": 1}`)
	if code, logs = h.serviceRun(cfg, func(int) bool { return true }, &removes); code != exitOK ||
		len(pick(logs, "run failed")) != 1 || !strings.Contains(fmt.Sprint(logs), "token file") {
		t.Fatalf("service = %d, logs %v", code, logs)
	}
}

func TestServiceCommand(t *testing.T) {
	h := newHarness(t)
	src := h.source(map[string]string{"a.txt": "alpha"})
	cfg := h.file("c.json", h.serviceConfig(src, ""), 0o600)
	if out := h.must(exitOK, "service", "--config", cfg, "--check"); !strings.Contains(out, "is valid: 1 sources, every 1h0m0s") {
		t.Errorf("--check = %q", out)
	}
	h.must(exitUsage, "service")
	h.must(exitUsage, "service", "--config", cfg, "extra")
	h.must(exitOK, "service", "-h")
	h.must(exitFailure, "service", "--config", filepath.Join(h.dir, "missing.json"))
	inline := h.file("inline.json", `{"passphrase": "`+passphrase+`"}`, 0o600)
	if code, _, errOut := h.run("", "service", "--config", inline); code != exitFailure || !strings.Contains(errOut, "holds a secret inline") {
		t.Errorf("an inline passphrase = %d: %s", code, errOut)
	}

	// The log goes to the log file, created private; a log file that cannot
	// be opened fails the service.
	logFile := filepath.Join(h.dir, "service.log")
	cfg = strings.Replace(h.serviceConfig(src, `, "log_file": "`+filepath.ToSlash(logFile)+`"`), `"log_format": "json"`, `"log_format": "text"`, 1)
	h.must(exitOK, "init", "--address", "backup.example:9000", "--repo", "hosts/a", "--passphrase-file", h.pass)
	if code, logs := h.serviceRun(cfg, func(int) bool { return true }, nil); code != exitOK || len(logs) != 0 {
		t.Fatalf("service = %d, stderr logs %v", code, logs)
	}
	b, err := os.ReadFile(logFile)
	if err != nil || !strings.Contains(string(b), "msg=\"snapshot saved\"") || strings.Contains(string(b), passphrase) {
		t.Fatalf("log file: %v\n%s", err, b)
	}
	cfg = strings.Replace(cfg, filepath.ToSlash(logFile), filepath.ToSlash(filepath.Join(h.dir, "no", "dir", "x.log")), 1)
	if code, _ := h.serviceRun(cfg, func(int) bool { return true }, nil); code != exitFailure {
		t.Fatalf("an unopenable log file = %d", code)
	}
}

func TestSecretFileAccess(t *testing.T) {
	h := newHarness(t)
	if _, err := readSecretFile(h.dir); !errors.Is(err, errSecretMode) {
		t.Errorf("a directory = %v, want errSecretMode", err)
	}
	if _, err := openRestricted(filepath.Join(h.dir, "missing"), othersNone, errSecretMode); err == nil || errors.Is(err, errSecretMode) {
		t.Errorf("a missing file = %v", err)
	}
	f, err := openRestricted(h.pass, othersWrite, errConfigMode)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
}
