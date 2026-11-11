package serverconfig

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
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
