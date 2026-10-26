package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	objstore "github.com/jeremyhahn/go-objstore/api/sdks/go"
	"github.com/jeremyhahn/go-objstore/backup/pkg/srcsnap"
	"github.com/jeremyhahn/go-objstore/backup/pkg/store"
)

const passphrase = "correct horse battery staple"

// harness runs commands against one in-memory store through the store seam
// and checks that no secret ever reaches the output.
type harness struct {
	t       *testing.T
	st      *store.Memory
	dir     string
	pass    string
	secrets []string
	openErr error
	closErr error
	snap    srcsnap.Env

	mu      sync.Mutex
	configs []*objstore.ClientConfig
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, st: store.NewMemory(), dir: t.TempDir(), secrets: []string{passphrase}}
	h.pass = h.file("pass", passphrase+"\n", 0o600)
	return h
}

func (h *harness) file(name, content string, mode os.FileMode) string {
	h.t.Helper()
	p := filepath.Join(h.dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		h.t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		h.t.Fatal(err)
	}
	return p
}

func (h *harness) open(_ context.Context, cfg *objstore.ClientConfig) (store.PruneStore, func() error, error) {
	h.mu.Lock()
	h.configs = append(h.configs, cfg)
	h.mu.Unlock()
	if h.openErr != nil {
		return nil, nil, h.openErr
	}
	return h.st, func() error { return h.closErr }, nil
}

// run runs args with stdin and returns the exit code and the output.
func (h *harness) run(stdin string, args ...string) (int, string, string) {
	h.t.Helper()
	var stdout bytes.Buffer
	code, errOut := h.runOut(&stdout, stdin, args...)
	for _, s := range h.secrets {
		if strings.Contains(stdout.String(), s) {
			h.t.Fatalf("%q: a secret reached the output", args)
		}
	}
	return code, stdout.String(), errOut
}

// runOut runs the CLI with stdout as its standard output.
func (h *harness) runOut(stdout io.Writer, stdin string, args ...string) (int, string) {
	h.t.Helper()
	var stderr bytes.Buffer
	e := &env{stdin: strings.NewReader(stdin), stdout: stdout, stderr: &stderr, open: h.open, snap: h.snap}
	code := runWith(context.Background(), e, args)
	for _, s := range h.secrets {
		if strings.Contains(stderr.String(), s) {
			h.t.Fatalf("%q: a secret reached the output", args)
		}
	}
	return code, stderr.String()
}

// failWriter is an output that refuses every write.
type failWriter struct{}

var errOutput = errors.New("output refused")

func (failWriter) Write([]byte) (int, error) { return 0, errOutput }

// mustFailOutput runs a command whose output refuses every write and checks it
// fails for that reason.
func (h *harness) mustFailOutput(args ...string) {
	h.t.Helper()
	code, errOut := h.runOut(failWriter{}, "", args...)
	if code != exitFailure || !strings.Contains(errOut, "write report: "+errOutput.Error()) {
		h.t.Errorf("%q with a failing output = %d, want %d\nstderr: %s", args, code, exitFailure, errOut)
	}
}

// must runs args and fails the test unless it exits with want.
func (h *harness) must(want int, args ...string) string {
	h.t.Helper()
	code, out, errOut := h.run("", args...)
	if code != want {
		h.t.Fatalf("%q = %d, want %d\nstdout: %s\nstderr: %s", args, code, want, out, errOut)
	}
	return out
}

func (h *harness) repo(args ...string) []string {
	return append(args, "--address", "backup.example:9000", "--repo", "hosts/a", "--passphrase-file", h.pass)
}

func (h *harness) pruner(args ...string) []string {
	return append(args, "--prune-address", "prune.example:9000", "--repo", "hosts/a", "--passphrase-file", h.pass)
}

func (h *harness) source(files map[string]string) string {
	h.t.Helper()
	root := h.t.TempDir()
	for name, content := range files {
		p := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			h.t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			h.t.Fatal(err)
		}
	}
	return root
}

func TestRun(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if got := run([]string{"--help"}, strings.NewReader(""), &stdout, &stderr); got != exitOK {
		t.Fatalf("run(--help) = %d", got)
	}
	if !strings.Contains(stdout.String(), "go-objstore Go SDK") {
		t.Errorf("help does not name the SDK: %q", stdout.String())
	}
}

func TestHelp(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStdout string
		wantStderr string
	}{
		{name: "no command", wantCode: exitUsage, wantStderr: "Usage: objstore-backup"},
		{name: "help", args: []string{"help"}, wantStdout: "Commands:"},
		{name: "help command", args: []string{"help", "restore"}, wantStdout: "--overwrite"},
		{name: "help unknown", args: []string{"help", "nope"}, wantCode: exitUsage, wantStderr: `unknown command "nope"`},
		{name: "unknown", args: []string{"nope"}, wantCode: exitUsage, wantStderr: `unknown command "nope"`},
		{name: "init -h", args: []string{"init", "-h"}, wantStdout: "--iterations"},
		{name: "backup -h", args: []string{"backup", "-h"}, wantStdout: "--passphrase-stdin"},
		{name: "snapshots -h", args: []string{"snapshots", "--help"}, wantStdout: "--json"},
		{name: "verify -h", args: []string{"verify", "-h"}, wantStdout: "Exits 3"},
		{name: "prune -h", args: []string{"prune", "-h"}, wantStdout: "delete-capable identity"},
		{name: "prune flags", args: []string{"help", "prune"}, wantStdout: "--prune-token-file"},
	}
	for _, tt := range tests {
		code, out, errOut := h.run("", tt.args...)
		if code != tt.wantCode {
			t.Errorf("%s: code %d, want %d", tt.name, code, tt.wantCode)
		}
		if !strings.Contains(out, tt.wantStdout) {
			t.Errorf("%s: stdout %q does not contain %q", tt.name, out, tt.wantStdout)
		}
		if !strings.Contains(errOut, tt.wantStderr) {
			t.Errorf("%s: stderr %q does not contain %q", tt.name, errOut, tt.wantStderr)
		}
	}
	if code, out, _ := h.run("", "prune", "-h"); code != exitOK || strings.Contains(out, "\n  --address ") {
		t.Errorf("prune help offers the backup identity's --address: %q", out)
	}
}

func TestUsageErrors(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	pass := []string{"--passphrase-file", h.pass}
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"no address", append([]string{"init"}, pass...), "--address is required"},
		{"no passphrase", []string{"init", "--address", "a:1"}, "a passphrase is required"},
		{"two passphrases", append([]string{"init", "--address", "a:1", "--passphrase-stdin"}, pass...), "exclusive"},
		{"bad protocol", append([]string{"init", "--address", "a:1", "--protocol", "ftp"}, pass...), `--protocol "ftp"`},
		{"iterations", h.repo("init", "--iterations", "1000"), "--iterations must be"},
		{"unknown flag", h.repo("snapshots", "--color"), "flag provided but not defined"},
		{"backup no path", h.repo("backup"), "missing arguments"},
		{"restore one arg", h.repo("restore", "abc"), "missing arguments"},
		{"restore three args", h.repo("restore", "abc", "t", "u"), `unexpected argument "u"`},
		{"snapshots arg", h.repo("snapshots", "x"), `unexpected argument "x"`},
		{"init bad timeout", h.repo("init", "--timeout", "soon"), "invalid value"},
		{"prune backup identity", h.repo("prune", "--keep-last", "1"), "-address"},
		{"prune no address", append([]string{"prune", "--keep-last", "1"}, pass...), "--prune-address is required"},
		{"prune keeps nothing", h.pruner("prune"), "keeps nothing"},
		{"prune negative", h.pruner("prune", "--keep-daily", "-1"), "negative"},
		{"prune grace", h.pruner("prune", "--keep-last", "1", "--grace", "0s"), "--grace must be positive"},
		{"prune within", h.pruner("prune", "--keep-within", "a week"), "invalid duration"},
		{"prune days", h.pruner("prune", "--keep-within", "xd"), "invalid number of days"},
		{"prune within days", h.pruner("prune", "--keep-within", "106752d"), "at most 106751"},
		{"prune within overflow", h.pruner("prune", "--keep-within", "213504d"), "at most 106751"},
		{"prune grace days", h.pruner("prune", "--keep-last", "1", "--grace", "106752d"), "at most 106751"},
		{"prune grace overflow", h.pruner("prune", "--keep-last", "1", "--grace", "213504d"), "at most 106751"},
	}
	for _, tt := range tests {
		code, _, errOut := h.run("", tt.args...)
		if code != exitUsage {
			t.Errorf("%s: code %d, want %d (%s)", tt.name, code, exitUsage, errOut)
		}
		if !strings.Contains(errOut, tt.want) {
			t.Errorf("%s: stderr %q does not contain %q", tt.name, errOut, tt.want)
		}
	}
	if len(h.configs) != 0 {
		t.Errorf("a usage error connected to a server %d times", len(h.configs))
	}
}

func TestEndToEnd(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	if out := h.must(exitOK, h.repo("init")...); !strings.Contains(out, `created repository`) || !strings.Contains(out, `"hosts/a/"`) {
		t.Fatalf("init: %q", out)
	}
	if _, _, errOut := h.run("", h.repo("init")...); !strings.Contains(errOut, "already exists") {
		t.Errorf("second init: %q", errOut)
	}

	first := h.source(map[string]string{"a.txt": "hello", "sub/b.txt": "world"})
	second := h.source(map[string]string{"c.txt": "again"})
	out := h.must(exitOK, h.repo("backup", first, "--host", "web-1")...)
	if !strings.Contains(out, "2 files, 2 dirs") {
		t.Errorf("backup: %q", out)
	}
	firstID := strings.Fields(out)[1]
	time.Sleep(10 * time.Millisecond)
	h.must(exitOK, h.repo("backup", second)...)

	out = h.must(exitOK, h.repo("snapshots")...)
	if !strings.Contains(out, firstID[:12]) || !strings.Contains(out, "web-1") {
		t.Errorf("snapshots: %q", out)
	}
	var list []snapshotInfo
	if err := json.Unmarshal([]byte(h.must(exitOK, h.repo("snapshots", "--json")...)), &list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ID.String() != firstID || list[0].Files != 2 || list[0].Bytes != 10 || list[0].Host != "web-1" {
		t.Fatalf("snapshots --json: %+v", list)
	}

	target := filepath.Join(t.TempDir(), "out")
	h.must(exitOK, h.repo("restore", firstID[:8], target)...)
	if got, err := os.ReadFile(filepath.Join(target, "sub", "b.txt")); err != nil || string(got) != "world" {
		t.Fatalf("restored b.txt = %q, %v", got, err)
	}
	h.must(exitFailure, h.repo("restore", firstID, target)...)
	h.must(exitOK, h.repo("restore", strings.ToUpper(firstID), target, "--overwrite")...)
	h.must(exitFailure, h.repo("restore", "ffffffffffff", target)...)
	h.must(exitFailure, h.repo("restore", "not-hex", target)...)
	h.must(exitFailure, h.repo("restore", strings.Repeat("z", 64), target)...)

	if out := h.must(exitOK, h.repo("verify")...); !strings.Contains(out, "no problems found") {
		t.Errorf("verify: %q", out)
	}
	h.must(exitOK, h.repo("verify", firstID[:6])...)
	h.must(exitFailure, h.repo("verify", "xyz")...)
	var vr verifyReport
	if err := json.Unmarshal([]byte(h.must(exitOK, h.repo("verify", "--json")...)), &vr); err != nil {
		t.Fatal(err)
	}
	if vr.Snapshots != 2 || vr.Files != 3 || len(vr.Problems) != 0 {
		t.Errorf("verify --json: %+v", vr)
	}

	// Retention applies to each host's snapshots of each directory on its
	// own: a second backup of first as web-1 makes firstID the one to forget,
	// and second, the other group's only snapshot, stays.
	againID := strings.Fields(h.must(exitOK, h.repo("backup", first, "--host", "web-1")...))[1]
	var pr pruneReport
	if err := json.Unmarshal([]byte(h.must(exitOK, h.pruner("prune", "--keep-last", "1", "--dry-run", "--json")...)), &pr); err != nil {
		t.Fatal(err)
	}
	if !pr.DryRun || len(pr.Kept) != 2 || len(pr.Forgotten) != 1 || pr.Forgotten[0].String() != firstID || len(pr.Groups) != 2 {
		t.Fatalf("prune --dry-run --json: %+v", pr)
	}
	for _, g := range pr.Groups {
		web := g.Host == "web-1"
		if root := map[bool]string{true: first, false: second}[web]; g.Root != root || len(g.Kept) != 1 ||
			(web && (g.Kept[0].String() != againID || len(g.Forgotten) != 1 || g.Forgotten[0].String() != firstID)) ||
			(!web && len(g.Forgotten) != 0) {
			t.Errorf("prune --dry-run --json group %+v", g)
		}
	}
	if out := h.must(exitOK, h.pruner("prune", "--dry-run", "--keep-within", "30d")...); !strings.Contains(out, "dry run") {
		t.Errorf("prune --dry-run: %q", out)
	}
	out = h.must(exitOK, h.pruner("prune", "--keep-last", "1", "--grace", "1d")...)
	if !strings.Contains(out, "forgot "+firstID) || !strings.Contains(out, "host web-1, "+first+": kept 1, forgot 1\n") ||
		!strings.Contains(out, "unused packs marked") {
		t.Errorf("prune: %q", out)
	}
	if err := json.Unmarshal([]byte(h.must(exitOK, h.repo("snapshots", "--json")...)), &list); err != nil || len(list) != 2 {
		t.Errorf("snapshots after prune: %+v, %v", list, err)
	}

	for _, cfg := range h.configs {
		if cfg.Address != "backup.example:9000" && cfg.Address != "prune.example:9000" {
			t.Errorf("connected to %q", cfg.Address)
		}
		if cfg.Protocol != objstore.ProtocolGRPC {
			t.Errorf("protocol %q", cfg.Protocol)
		}
		if cfg.MaxRecvMsgSize != maxMessage || cfg.MaxSendMsgSize != maxMessage {
			t.Errorf("gRPC message limits %d, %d", cfg.MaxRecvMsgSize, cfg.MaxSendMsgSize)
		}
	}
	if last := h.configs[len(h.configs)-2]; last.Address != "prune.example:9000" {
		t.Errorf("prune connected to %q, not its own identity's server", last.Address)
	}
}

func TestVerifyProblems(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.must(exitOK, h.repo("init")...)
	h.must(exitOK, h.repo("backup", h.source(map[string]string{"a.txt": "hello"}))...)
	var packs []string
	h.st.Each(func(name string, _ []byte) {
		if strings.Contains(name, "/packs/") {
			packs = append(packs, name)
		}
	})
	if len(packs) == 0 {
		t.Fatal("no pack written")
	}
	h.st.Replace(packs[0], []byte("not a pack"))

	if out := h.must(exitProblems, h.repo("verify")...); !strings.Contains(out, "problem: ") {
		t.Errorf("verify: %q", out)
	}
	var vr verifyReport
	if err := json.Unmarshal([]byte(h.must(exitProblems, h.repo("verify", "--json")...)), &vr); err != nil {
		t.Fatal(err)
	}
	if len(vr.Problems) == 0 || vr.Problems[0].Kind == "" || vr.Problems[0].Error == "" {
		t.Errorf("verify --json problems: %+v", vr.Problems)
	}
	h.mustFailOutput(h.repo("verify")...)
	h.mustFailOutput(h.repo("verify", "--json")...)
	h.must(exitFailure, h.repo("restore", "0", t.TempDir())...)

	h.st.Delete(packs[0])
	h.must(exitProblems, h.repo("verify")...)
}

func TestOutputFailures(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.mustFailOutput(h.repo("init")...)
	src := h.source(map[string]string{"a.txt": "hello"})
	h.mustFailOutput(h.repo("backup", src)...)
	id := strings.Fields(h.must(exitOK, h.repo("backup", src)...))[1]
	for _, args := range [][]string{
		h.repo("snapshots"),
		h.repo("snapshots", "--json"),
		h.repo("restore", id, filepath.Join(t.TempDir(), "out")),
		h.repo("verify"),
		h.repo("verify", "--json"),
		h.pruner("prune", "--keep-last", "1", "--dry-run"),
		h.pruner("prune", "--keep-last", "1", "--dry-run", "--json"),
		h.pruner("prune", "--keep-last", "1"),
	} {
		h.mustFailOutput(args...)
	}
	// The failed reports changed nothing the commands do: prune forgot the
	// older snapshot.
	if out := h.must(exitOK, h.repo("snapshots")...); !strings.Contains(out, id[:12]) || strings.Count(out, "\n") != 2 {
		t.Errorf("snapshots after prune: %q", out)
	}
}

func TestPassphrase(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	init := func(stdin string, args ...string) (int, string) {
		code, _, errOut := h.run(stdin, append([]string{"init", "--address", "a:1"}, args...)...)
		return code, errOut
	}
	long := strings.Repeat("p", maxSecret+1)
	h.secrets = append(h.secrets, "group-readable", "other-readable", long, "from stdin s3cret", "wrong passphrase")
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"group bits", []string{"--passphrase-file", h.file("g", "group-readable", 0o640)}, "no group or other permissions"},
		{"other bits", []string{"--passphrase-file", h.file("o", "other-readable", 0o604)}, "no group or other permissions"},
		{"directory", []string{"--passphrase-file", h.dir}, "no group or other permissions"},
		{"missing", []string{"--passphrase-file", filepath.Join(h.dir, "none")}, "no such file"},
		{"empty", []string{"--passphrase-file", h.file("e", "\r\n", 0o600)}, "is empty"},
		{"too long", []string{"--passphrase-file", h.file("l", long, 0o600)}, "longer than"},
	}
	for _, tt := range tests {
		code, errOut := init("", tt.args...)
		if code != exitFailure || !strings.Contains(errOut, tt.want) {
			t.Errorf("%s: code %d, stderr %q, want %q", tt.name, code, errOut, tt.want)
		}
	}
	if len(h.configs) != 0 {
		t.Errorf("a refused passphrase connected to a server")
	}
	if code, errOut := init("", "--passphrase-stdin"); code != exitFailure || !strings.Contains(errOut, "from standard input: is empty") {
		t.Errorf("empty stdin: %d %q", code, errOut)
	}

	if code, errOut := init("from stdin s3cret\n", "--passphrase-stdin"); code != exitOK {
		t.Fatalf("init from stdin: %d %q", code, errOut)
	}
	if code, _, errOut := h.run("from stdin s3cret", "snapshots", "--address", "a:1", "--passphrase-stdin"); code != exitOK {
		t.Errorf("snapshots from stdin: %d %q", code, errOut)
	}
	code, _, errOut := h.run("wrong passphrase", "snapshots", "--address", "a:1", "--passphrase-stdin")
	if code != exitFailure || !strings.Contains(errOut, "objstore-backup snapshots: ") {
		t.Errorf("wrong passphrase: %d %q", code, errOut)
	}
}

func TestToken(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.secrets = append(h.secrets, "bearer-xyz")
	h.must(exitOK, h.repo("init", "--token-file", h.file("tok", "bearer-xyz\n", 0o600), "--tenant", "t1", "--tls")...)
	if cfg := h.configs[0]; cfg.Token != "bearer-xyz" || cfg.TenantID != "t1" || !cfg.UseTLS {
		t.Errorf("config: token set %v, tenant %q, tls %v", cfg.Token == "bearer-xyz", cfg.TenantID, cfg.UseTLS)
	}
	code, _, errOut := h.run("", h.repo("snapshots", "--token-file", h.file("tok2", "bearer-xyz", 0o644))...)
	if code != exitFailure || !strings.Contains(errOut, "token file") {
		t.Errorf("readable token file: %d %q", code, errOut)
	}
}

func TestStoreFailures(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.must(exitFailure, h.repo("snapshots")...)
	h.must(exitFailure, h.pruner("prune", "--keep-last", "1")...)
	h.must(exitOK, h.repo("init")...)

	h.closErr = errors.New("close refused")
	if _, _, errOut := h.run("", h.repo("snapshots")...); !strings.Contains(errOut, "close refused") {
		t.Errorf("close error: %q", errOut)
	}
	h.must(exitFailure, h.repo("init")...)
	h.must(exitFailure, h.pruner("prune", "--keep-last", "1")...)
	h.closErr = nil

	h.must(exitFailure, h.repo("backup", filepath.Join(h.dir, "missing"))...)

	h.openErr = errors.New("connection refused")
	for _, args := range [][]string{h.repo("init"), h.repo("verify"), h.pruner("prune", "--keep-last", "1")} {
		if code, _, errOut := h.run("", args...); code != exitFailure || !strings.Contains(errOut, "connection refused") {
			t.Errorf("%q: %d %q", args, code, errOut)
		}
	}
	h.openErr = nil
	h.must(exitFailure, h.repo("init", "--token-file", filepath.Join(h.dir, "none"))...)
	h.must(exitFailure, h.pruner("prune", "--keep-last", "1", "--prune-token-file", filepath.Join(h.dir, "none"))...)
	h.must(exitFailure, h.pruner("prune", "--keep-last", "1", "--passphrase-file", filepath.Join(h.dir, "none"))...)
}

func TestOpenSDK(t *testing.T) {
	if _, _, err := openSDK(context.Background(), &objstore.ClientConfig{Protocol: "ftp"}); err == nil {
		t.Error("openSDK accepted an unknown protocol")
	}
	st, closer, err := openSDK(context.Background(), &objstore.ClientConfig{
		Protocol: objstore.ProtocolREST, Address: "http://127.0.0.1:1", ConnectionTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("openSDK: %v", err)
	}
	if st == nil {
		t.Fatal("openSDK returned no store")
	}
	if err := closer(); err != nil {
		t.Errorf("close: %v", err)
	}
}

func TestParse(t *testing.T) {
	var stdout, stderr bytes.Buffer
	e := &env{stdout: &stdout, stderr: &stderr}
	fs := newFlags("x")
	b := fs.Bool("b", false, "")
	pos, _, ok := e.parse("x", "", fs, []string{"one", "-b", "--", "-two", "-b"}, 0, -1)
	if !ok || !*b || strings.Join(pos, " ") != "one -two -b" {
		t.Errorf("parse = %q, %v, b %v", pos, ok, *b)
	}
}

func TestDays(t *testing.T) {
	var d time.Duration
	f := days{&d}
	for in, want := range map[string]time.Duration{
		"2d": 48 * time.Hour, "90m": 90 * time.Minute, "0d": 0,
		"106751d": 106751 * 24 * time.Hour,
	} {
		if err := f.Set(in); err != nil || d != want {
			t.Errorf("Set(%q) = %v, %v", in, d, err)
		}
	}
	if maxDays != 106751 {
		t.Errorf("maxDays = %d", maxDays)
	}
	for _, in := range []string{"-1d", "d", "1w", "", "106752d", "213504d", "99999999999999999999d"} {
		d = time.Hour
		if err := f.Set(in); err == nil || d != time.Hour {
			t.Errorf("Set(%q) accepted: %v", in, d)
		}
	}
	if (days{}).String() != "" || f.String() != "1h0m0s" {
		t.Errorf("String = %q, %q", (days{}).String(), f.String())
	}
}

type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestReadSecret(t *testing.T) {
	if _, err := readSecret(failReader{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("readSecret error = %v", err)
	}
	got, err := readSecret(strings.NewReader("pw\r\n"))
	if err != nil || string(got) != "pw" {
		t.Errorf("readSecret = %q, %v", got, err)
	}
	if err := (&secretError{what: "passphrase", source: "x", err: errSecretEmpty}); !errors.Is(err, errSecretEmpty) {
		t.Error("secretError does not unwrap")
	}
}
