package serverconfig

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

func TestEnableReplication_EmptyPathEnablesNothing(t *testing.T) {
	// Run in an empty working directory: a policy path built from an empty
	// cfg.Path would land here.
	dir := t.TempDir()
	t.Chdir(dir)

	cfg := Defaults()
	cfg.Path = ""
	if got := cfg.ReplicationPolicyPath(); got != "" {
		t.Fatalf("ReplicationPolicyPath() = %q, want empty", got)
	}

	called := false
	if cfg.EnableReplication(func(string, *objstore.ReplicationConfig) error {
		called = true
		return nil
	}) {
		t.Fatal("EnableReplication reported enabled with no path")
	}
	if called {
		t.Fatal("enabler called with no path")
	}

	// The real enabler is not reached either: no file is opened or created.
	if cfg.EnableReplication(nil) {
		t.Fatal("EnableReplication(nil) reported enabled with no path")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("working directory has %d entries, want none (first %q)", len(entries), entries[0].Name())
	}
}

func TestEnableReplication_PathUsesFileUnderIt(t *testing.T) {
	cfg := Defaults()
	cfg.Path = filepath.Join(t.TempDir(), "data")

	want := filepath.Join(cfg.Path, common.ReplicationPolicyFile)
	if got := cfg.ReplicationPolicyPath(); got != want {
		t.Fatalf("ReplicationPolicyPath() = %q, want %q", got, want)
	}

	var gotBackend string
	var gotCfg *objstore.ReplicationConfig
	if !cfg.EnableReplication(func(backend string, rc *objstore.ReplicationConfig) error {
		gotBackend, gotCfg = backend, rc
		return nil
	}) {
		t.Fatal("EnableReplication reported not enabled")
	}
	if gotBackend != "" {
		t.Errorf("backend = %q, want the default backend", gotBackend)
	}
	if gotCfg == nil || gotCfg.PolicyFilePath != want {
		t.Fatalf("policy file = %+v, want %q", gotCfg, want)
	}
	if gotCfg.RunInBackground {
		t.Error("RunInBackground = true, want false")
	}

	if cfg.EnableReplication(func(string, *objstore.ReplicationConfig) error {
		return errors.New("no replication manager")
	}) {
		t.Error("EnableReplication reported enabled on an enabler error")
	}
}

// addLocalPolicyWith builds a replication manager from the ReplicationConfig
// EnableReplication passes on and adds a policy with a local destination.
func addLocalPolicyWith(t *testing.T, cfg *Config) error {
	t.Helper()
	var addErr error
	if !cfg.EnableReplication(func(_ string, rc *objstore.ReplicationConfig) error {
		mgr, err := replication.NewPersistentReplicationManager(nil, rc.PolicyFilePath, 0, nil, nil,
			replication.WithClientLocalBackends(rc.AllowClientLocalBackends))
		if err != nil {
			return err
		}
		addErr = mgr.AddPolicy(common.ReplicationPolicy{
			ID:                  "p1",
			SourceBackend:       "memory",
			DestinationBackend:  "local",
			DestinationSettings: map[string]string{"path": t.TempDir()},
		})
		return nil
	}) {
		t.Fatal("EnableReplication reported not enabled")
	}
	return addErr
}

func TestEnableReplication_DefaultRefusesClientLocalBackends(t *testing.T) {
	cfg := Defaults()
	cfg.Path = t.TempDir()
	if cfg.Replication.AllowClientLocalBackends {
		t.Fatal("replication.allow_client_local_backends defaults to true, want false")
	}
	if err := addLocalPolicyWith(t, &cfg); !errors.Is(err, replication.ErrClientLocalBackend) {
		t.Fatalf("AddPolicy(local) with the default = %v, want ErrClientLocalBackend", err)
	}

	cfg.Replication.AllowClientLocalBackends = true
	if err := addLocalPolicyWith(t, &cfg); err != nil {
		t.Fatalf("AddPolicy(local) with allow_client_local_backends = %v, want nil", err)
	}
}

func TestLoad_ReplicationAllowClientLocalBackends(t *testing.T) {
	if cfg := mustLoad(t, nil, Defaults(), BindServerFlags); cfg.Replication.AllowClientLocalBackends {
		t.Error("default allow_client_local_backends = true, want false")
	}
	file := writeConfig(t, "replication:\n  allow_client_local_backends: true\n")
	if cfg := mustLoad(t, []string{"-config", file}, Defaults(), BindServerFlags); !cfg.Replication.AllowClientLocalBackends {
		t.Error("config file did not set replication.allow_client_local_backends")
	}
	if cfg := mustLoad(t, []string{"-replication-allow-client-local-backends"}, Defaults(), BindServerFlags); !cfg.Replication.AllowClientLocalBackends {
		t.Error("flag did not set replication.allow_client_local_backends")
	}
	t.Setenv("OBJSTORE_REPLICATION_ALLOW_CLIENT_LOCAL_BACKENDS", "true")
	if cfg := mustLoad(t, nil, Defaults(), BindServerFlags); !cfg.Replication.AllowClientLocalBackends {
		t.Error("environment did not set replication.allow_client_local_backends")
	}
}

// TestEnableReplication_Interval: with replication.interval the enabler gets
// RunInBackground, the interval and the caller's context; without one,
// nothing runs in the background (#222).
func TestEnableReplication_Interval(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "server")

	capture := func(cfg *Config) *objstore.ReplicationConfig {
		t.Helper()
		var got *objstore.ReplicationConfig
		if !cfg.EnableReplicationContext(ctx, func(_ string, rc *objstore.ReplicationConfig) error {
			got = rc
			return nil
		}) {
			t.Fatal("EnableReplicationContext reported not enabled")
		}
		return got
	}

	cfg := Defaults()
	cfg.Path = t.TempDir()
	if rc := capture(&cfg); rc.RunInBackground || rc.Interval != 0 || rc.Context != nil {
		t.Errorf("no interval: RunInBackground=%v Interval=%v Context=%v, want trigger-only", rc.RunInBackground, rc.Interval, rc.Context)
	}

	cfg.Replication.Interval = 15 * time.Minute
	rc := capture(&cfg)
	if !rc.RunInBackground || rc.Interval != 15*time.Minute {
		t.Errorf("interval 15m: RunInBackground=%v Interval=%v, want true/15m", rc.RunInBackground, rc.Interval)
	}
	if rc.Context == nil || rc.Context.Value(ctxKey{}) != "server" {
		t.Error("interval set: the enabler did not get the caller's context")
	}
}

// TestEnableReplicationContext_ScheduleCopiesThenStops drives the real
// enabler, objstore.EnableReplication: with an interval set, a scheduled sync
// copies a source object to the destination with no trigger, and once the
// context is cancelled no further copy happens (#222).
func TestEnableReplicationContext_ScheduleCopiesThenStops(t *testing.T) {
	srcDir, destDir := t.TempDir(), t.TempDir()
	source, err := factory.NewStorage("local", map[string]string{"path": srcDir}, common.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := factory.NewStorage("local", map[string]string{"path": destDir}, common.Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	objstore.Reset()
	t.Cleanup(objstore.Reset)
	if err := objstore.Initialize(&objstore.FacadeConfig{
		Backends:       map[string]common.Storage{"default": source},
		DefaultBackend: "default",
	}); err != nil {
		t.Fatal(err)
	}

	cfg := Defaults()
	cfg.Path = t.TempDir()
	cfg.Replication.AllowClientLocalBackends = true
	cfg.Replication.Interval = 20 * time.Millisecond
	mgr, err := replication.NewPersistentReplicationManager(&replication.OSFileSystem{},
		cfg.ReplicationPolicyPath(), 0, nil, nil, replication.WithClientLocalBackends(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.AddPolicy(common.ReplicationPolicy{
		ID:                  "scheduled",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": srcDir},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": destDir},
		ReplicationMode:     common.ReplicationModeTransparent,
		Enabled:             true,
	}); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := source.PutWithContext(ctx, "before", bytes.NewReader([]byte("1"))); err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if !cfg.EnableReplicationContext(runCtx, nil) {
		t.Fatal("EnableReplicationContext reported not enabled")
	}

	copied := func(key string) bool {
		ok, err := dest.Exists(ctx, key)
		return err == nil && ok
	}
	deadline := time.Now().Add(10 * time.Second)
	for !copied("before") {
		if time.Now().After(deadline) {
			t.Fatal("no scheduled copy within 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	time.Sleep(200 * time.Millisecond) // let an in-flight sync finish
	if err := source.PutWithContext(ctx, "after", bytes.NewReader([]byte("2"))); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * cfg.Replication.Interval)
	if copied("after") {
		t.Fatal("a copy happened after the context was cancelled")
	}
}

func TestLoad_ReplicationInterval(t *testing.T) {
	if cfg := mustLoad(t, nil, Defaults(), BindServerFlags); cfg.Replication.Interval != 0 {
		t.Errorf("default replication.interval = %v, want 0", cfg.Replication.Interval)
	}
	file := writeConfig(t, "replication:\n  interval: 10m\n")
	if cfg := mustLoad(t, []string{"-config", file}, Defaults(), BindServerFlags); cfg.Replication.Interval != 10*time.Minute {
		t.Errorf("file replication.interval = %v, want 10m", cfg.Replication.Interval)
	}
	t.Setenv("OBJSTORE_REPLICATION_INTERVAL", "20m")
	if cfg := mustLoad(t, []string{"-config", file}, Defaults(), BindServerFlags); cfg.Replication.Interval != 20*time.Minute {
		t.Errorf("env replication.interval = %v, want 20m", cfg.Replication.Interval)
	}
	if cfg := mustLoad(t, []string{"-replication-interval", "30s"}, Defaults(), BindServerFlags); cfg.Replication.Interval != 30*time.Second {
		t.Errorf("flag replication.interval = %v, want 30s", cfg.Replication.Interval)
	}

	_, err := Load("test", []string{"-replication-interval", "-1m"}, Defaults(), BindServerFlags)
	if keys := invalidKeys(err); len(keys) != 1 || keys[0] != "replication.interval" {
		t.Errorf("negative interval: invalid keys = %v, want [replication.interval] (err %v)", keys, err)
	}
}
