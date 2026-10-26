package main

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/backup/pkg/prune"
	"github.com/jeremyhahn/go-objstore/backup/pkg/srcsnap"
)

// validConfig is a configuration with every setting; tests replace parts.
const validConfig = `{
  "connection": {"protocol": "rest", "address": "backup.example:9000", "tls": true,
    "ca_file": "/etc/ca.pem", "cert_file": "/etc/c.pem", "key_file": "/etc/k.pem",
    "token_file": "/etc/token", "tenant": "t1", "timeout": "30s"},
  "repo": "hosts/a",
  "passphrase_file": "/etc/objstore-backup/passphrase",
  "sources": ["/srv/data", "/etc"],
  "host": "web1",
  "schedule": {"daily": "02:30", "jitter": "15m"},
  "prune": {"connection": {"address": "prune.example:9000", "token_file": "/etc/prune-token"},
    "keep_last": 3, "keep_within": "30d", "keep_daily": 7, "keep_weekly": 4, "keep_monthly": 6, "keep_yearly": 2,
    "grace": "2d"},
  "log_file": "/var/log/objstore-backup.log",
  "log_format": "json",
  "source_snapshot": {"method": "auto", "on_failure": "fallback", "dir": "/run/snaps",
    "lvm": {"size": "4G"}, "btrfs": {"dir": "/srv/.snapshots"}}
}`

func TestConfigValid(t *testing.T) {
	h := newHarness(t)
	c, err := loadConfig(h.file("config.json", validConfig, 0o644))
	if err != nil {
		t.Fatal(err)
	}
	want := connFlags{
		prefix: "connection.", protocol: "rest", address: "backup.example:9000", tls: true,
		caFile: "/etc/ca.pem", certFile: "/etc/c.pem", keyFile: "/etc/k.pem",
		tokenFile: "/etc/token", tenant: "t1", timeout: 30 * time.Second,
	}
	if c.conn != want {
		t.Errorf("connection = %+v, want %+v", c.conn, want)
	}
	if c.repo != (repoFlags{repo: "hosts/a", passFile: "/etc/objstore-backup/passphrase"}) {
		t.Errorf("repository = %+v", c.repo)
	}
	if len(c.sources) != 2 || c.host != "web1" || c.logFile == "" || c.logFormat != "json" {
		t.Errorf("config = %+v", c)
	}
	if c.schedule.String() != "daily at 02:30" || c.schedule.Jitter() != 15*time.Minute {
		t.Errorf("schedule = %v, jitter %v", c.schedule, c.schedule.Jitter())
	}
	p := c.prune
	if p == nil || p.conn.address != "prune.example:9000" || p.conn.protocol != "grpc" || p.conn.tokenFile != "/etc/prune-token" {
		t.Fatalf("prune = %+v", p)
	}
	wantPol := prune.Policy{Last: 3, Within: 30 * 24 * time.Hour, Daily: 7, Weekly: 4, Monthly: 6, Yearly: 2}
	if p.policy != wantPol || p.opts.Grace != 48*time.Hour {
		t.Errorf("policy = %+v, grace %v", p.policy, p.opts.Grace)
	}
	wantSnap := srcsnap.Config{Method: srcsnap.Auto, OnFailure: srcsnap.Fallback, Dir: "/run/snaps", LVMSize: "4G", BtrfsDir: "/srv/.snapshots"}
	if c.snap != wantSnap {
		t.Errorf("source_snapshot = %+v, want %+v", c.snap, wantSnap)
	}

	// The defaults: grpc, an interval, no prune, the default grace.
	c, err = parseConfig("c", []byte(`{"connection": {"address": "a:1"}, "passphrase_file": "/p",
		"sources": ["/s"], "schedule": {"every": "6h"}, "prune": {"connection": {"address": "b:1"}, "keep_last": 1}}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.conn.protocol != "grpc" || c.schedule.String() != "every 6h0m0s" || c.prune.opts.Grace != prune.DefaultGrace ||
		c.snap.Method != srcsnap.None || c.snap.OnFailure != srcsnap.Fail {
		t.Errorf("defaults = %+v", c)
	}
}

func TestConfigExample(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "packaging", "config.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := parseConfig("config.example.json", b)
	if err != nil {
		t.Fatal(err)
	}
	if c.prune == nil || c.prune.conn.tokenFile == c.conn.tokenFile {
		t.Error("the example does not prune with its own identity")
	}
}

func TestConfigRefused(t *testing.T) {
	base := `"connection": {"address": "a:1"}, "passphrase_file": "/p", "sources": ["/s"]`
	sched := `"schedule": {"every": "1h"}`
	for _, tc := range []struct {
		name, body string
		want       error
		setting    string
	}{
		{"unknown key", `{` + base + `, ` + sched + `, "exclude": ["/tmp"]}`, errConfigUnknown, `"exclude"`},
		{"unknown nested key", `{"connection": {"address": "a:1", "passwd": "x"}, "passphrase_file": "/p", "sources": ["/s"], ` + sched + `}`, errConfigUnknown, `"passwd"`},
		{"inline passphrase", `{` + base + `, ` + sched + `, "passphrase": "hunter2"}`, errConfigInline, "passphrase"},
		{"inline token", `{"connection": {"address": "a:1", "token": "tok"}, "passphrase_file": "/p", "sources": ["/s"], ` + sched + `}`, errConfigInline, "connection.token"},
		{"inline prune token", `{` + base + `, ` + sched + `, "prune": {"connection": {"address": "b:1", "token": "tok"}, "keep_last": 1}}`, errConfigInline, "prune.connection.token"},
		{"no connection", `{"passphrase_file": "/p", "sources": ["/s"], ` + sched + `}`, errConfigMissing, "connection"},
		{"no address", `{"connection": {}, "passphrase_file": "/p", "sources": ["/s"], ` + sched + `}`, errConfigMissing, "connection.address"},
		{"bad protocol", `{"connection": {"address": "a:1", "protocol": "ftp"}, "passphrase_file": "/p", "sources": ["/s"], ` + sched + `}`, errConfigInvalid, "connection.protocol"},
		{"negative timeout", `{"connection": {"address": "a:1", "timeout": "-1s"}, "passphrase_file": "/p", "sources": ["/s"], ` + sched + `}`, errConfigInvalid, "connection.timeout"},
		{"no passphrase file", `{"connection": {"address": "a:1"}, "sources": ["/s"], ` + sched + `}`, errConfigMissing, "passphrase_file"},
		{"no sources", `{"connection": {"address": "a:1"}, "passphrase_file": "/p", ` + sched + `}`, errConfigMissing, "sources"},
		{"relative source", `{"connection": {"address": "a:1"}, "passphrase_file": "/p", "sources": ["data"], ` + sched + `}`, errConfigInvalid, "sources[0]"},
		{"no schedule", `{` + base + `}`, errConfigMissing, "schedule"},
		{"empty schedule", `{` + base + `, "schedule": {}}`, errConfigMissing, "schedule"},
		{"both schedules", `{` + base + `, "schedule": {"every": "1h", "daily": "02:00"}}`, errConfigInvalid, "exclusive"},
		{"bad daily", `{` + base + `, "schedule": {"daily": "25:00"}}`, errConfigInvalid, "schedule.daily"},
		{"short interval", `{` + base + `, "schedule": {"every": "30s"}}`, errConfigInvalid, "schedule.every"},
		{"long jitter", `{` + base + `, "schedule": {"every": "1h", "jitter": "2h"}}`, errConfigInvalid, "schedule.jitter"},
		{"bad duration", `{` + base + `, "schedule": {"every": "soon"}}`, errConfigInvalid, "invalid duration"},
		{"number duration", `{` + base + `, "schedule": {"every": 60}}`, errConfigInvalid, "a duration must be a string"},
		{"bad log format", `{` + base + `, ` + sched + `, "log_format": "xml"}`, errConfigInvalid, "log_format"},
		{"wrong type", `{` + base + `, ` + sched + `, "host": 7}`, errConfigInvalid, "host must be a JSON string"},
		{"prune without connection", `{` + base + `, ` + sched + `, "prune": {"keep_last": 1}}`, errConfigMissing, "prune.connection"},
		{"prune keeps nothing", `{` + base + `, ` + sched + `, "prune": {"connection": {"address": "b:1"}}}`, errConfigMissing, "keeps nothing"},
		{"prune negative keep", `{` + base + `, ` + sched + `, "prune": {"connection": {"address": "b:1"}, "keep_last": -1}}`, errConfigInvalid, "negative"},
		{"prune zero grace", `{` + base + `, ` + sched + `, "prune": {"connection": {"address": "b:1"}, "keep_last": 1, "grace": "0s"}}`, errConfigInvalid, "prune.grace"},
		{"unknown snapshot method", `{` + base + `, ` + sched + `, "source_snapshot": {"method": "fsfreeze"}}`, srcsnap.ErrConfig, "source_snapshot"},
		{"bad snapshot policy", `{` + base + `, ` + sched + `, "source_snapshot": {"method": "zfs", "on_failure": "retry"}}`, errConfigInvalid, "source_snapshot"},
		{"bad lvm size", `{` + base + `, ` + sched + `, "source_snapshot": {"method": "lvm", "lvm": {"size": "4G; rm"}}}`, srcsnap.ErrConfig, "source_snapshot"},
		{"lvm size for zfs", `{` + base + `, ` + sched + `, "source_snapshot": {"method": "zfs", "lvm": {"size": "4G"}}}`, errConfigInvalid, "source_snapshot"},
		{"relative btrfs dir", `{` + base + `, ` + sched + `, "source_snapshot": {"method": "btrfs", "btrfs": {"dir": "snaps"}}}`, errConfigInvalid, "source_snapshot"},
		{"relative snapshot dir", `{` + base + `, ` + sched + `, "source_snapshot": {"method": "auto", "dir": "run"}}`, errConfigInvalid, "source_snapshot"},
		{"unknown snapshot key", `{` + base + `, ` + sched + `, "source_snapshot": {"method": "zfs", "freeze": true}}`, errConfigUnknown, `"freeze"`},
		{"syntax", `{` + base + `,}`, errConfigSyntax, "at byte"},
		{"truncated", `{` + base, errConfigSyntax, "ends early"},
		{"trailing data", `{` + base + `, ` + sched + `} {}`, errConfigSyntax, "data after"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseConfig("/etc/c.json", []byte(tc.body))
			var ce *configError
			if !errors.Is(err, tc.want) || !errors.As(err, &ce) || !strings.Contains(err.Error(), tc.setting) {
				t.Fatalf("parseConfig = %v, want %v naming %q", err, tc.want, tc.setting)
			}
			for _, secret := range []string{"hunter2", `"tok"`} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("the error %q repeats a secret", err)
				}
			}
		})
	}
}

func TestConfigFile(t *testing.T) {
	h := newHarness(t)
	if _, err := loadConfig(filepath.Join(h.dir, "missing.json")); err == nil || errors.Is(err, errConfigMode) {
		t.Errorf("a missing file = %v", err)
	}
	big := h.file("big.json", `{"host": "`+strings.Repeat("x", maxConfig)+`"}`, 0o600)
	if _, err := loadConfig(big); !errors.Is(err, errConfigInvalid) {
		t.Errorf("a file over 1 MiB = %v, want errConfigInvalid", err)
	}
	if _, err := loadConfig(h.dir); !errors.Is(err, errConfigMode) {
		t.Errorf("a directory = %v, want errConfigMode", err)
	}
	if runtime.GOOS == "windows" {
		t.Skip("permission bits do not govern access on Windows")
	}
	for _, mode := range []os.FileMode{0o664, 0o646, 0o666} {
		p := h.file("w.json", validConfig, mode)
		if _, err := loadConfig(p); !errors.Is(err, errConfigMode) {
			t.Errorf("a config file of mode %v = %v, want errConfigMode", mode, err)
		}
	}
	// Readable by others is fine: the file holds no secret.
	if _, err := loadConfig(h.file("r.json", validConfig, 0o644)); err != nil {
		t.Errorf("a config file of mode 0644 = %v", err)
	}
}
