package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	objstore "github.com/jeremyhahn/go-objstore/api/sdks/go"
	"github.com/jeremyhahn/go-objstore/backup/internal/schedule"
	"github.com/jeremyhahn/go-objstore/backup/pkg/prune"
	"github.com/jeremyhahn/go-objstore/backup/pkg/srcsnap"
)

// maxConfig is the largest configuration file the service reads.
const maxConfig = 1 << 20

var (
	errConfigMode    = errors.New("must be a regular file that group and others cannot write")
	errConfigSyntax  = errors.New("is not valid JSON")
	errConfigUnknown = errors.New("has an unknown setting")
	errConfigMissing = errors.New("lacks a required setting")
	errConfigInvalid = errors.New("has an invalid setting")
	errConfigInline  = errors.New("holds a secret inline; name a file that holds it instead")
)

// configError is a configuration file the service refuses. It names the
// file and the setting, never a setting's value.
type configError struct {
	path    string
	setting string
	err     error
}

func (e *configError) Error() string {
	msg := "config file " + e.path + " " + e.err.Error()
	if e.setting != "" {
		msg += ": " + e.setting
	}
	return msg
}

func (e *configError) Unwrap() error { return e.err }

// duration is a JSON string holding a Go duration or a whole number of days
// ("30d").
type duration time.Duration

func (d *duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return errors.New("a duration must be a string such as \"6h\" or \"30d\"")
	}
	var v time.Duration
	if err := (days{&v}).Set(s); err != nil {
		return err
	}
	*d = duration(v)
	return nil
}

// connConfig is one identity's server connection: the settings the CLI's
// connection flags take.
type connConfig struct {
	Protocol  string          `json:"protocol"`
	Address   string          `json:"address"`
	TLS       bool            `json:"tls"`
	CAFile    string          `json:"ca_file"`
	CertFile  string          `json:"cert_file"`
	KeyFile   string          `json:"key_file"`
	TokenFile string          `json:"token_file"`
	Token     json.RawMessage `json:"token"`
	Tenant    string          `json:"tenant"`
	Timeout   duration        `json:"timeout"`
}

type scheduleConfig struct {
	Every  duration `json:"every"`
	Daily  string   `json:"daily"`
	Jitter duration `json:"jitter"`
}

type pruneConfig struct {
	Connection  *connConfig `json:"connection"`
	KeepLast    int         `json:"keep_last"`
	KeepWithin  duration    `json:"keep_within"`
	KeepDaily   int         `json:"keep_daily"`
	KeepWeekly  int         `json:"keep_weekly"`
	KeepMonthly int         `json:"keep_monthly"`
	KeepYearly  int         `json:"keep_yearly"`
	Grace       *duration   `json:"grace"`
}

// fileConfig is the configuration file's shape.
type fileConfig struct {
	Connection     *connConfig     `json:"connection"`
	Repo           string          `json:"repo"`
	PassphraseFile string          `json:"passphrase_file"`
	Passphrase     json.RawMessage `json:"passphrase"`
	Sources        []string        `json:"sources"`
	Host           string          `json:"host"`
	Schedule       *scheduleConfig `json:"schedule"`
	Prune          *pruneConfig    `json:"prune"`
	LogFile        string          `json:"log_file"`
	LogFormat      string          `json:"log_format"`
	SourceSnapshot *snapshotConfig `json:"source_snapshot"`
}

// snapshotConfig is the source_snapshot object: how each run snapshots its
// sources before reading them.
type snapshotConfig struct {
	Method    string `json:"method"`
	OnFailure string `json:"on_failure"`
	Dir       string `json:"dir"`
	LVM       *struct {
		Size string `json:"size"`
	} `json:"lvm"`
	Btrfs *struct {
		Dir string `json:"dir"`
	} `json:"btrfs"`
}

// serviceConfig is a checked configuration.
type serviceConfig struct {
	conn      connFlags
	repo      repoFlags
	sources   []string
	host      string
	schedule  schedule.Schedule
	prune     *servicePrune
	logFile   string
	logFormat string
	snap      srcsnap.Config
}

// servicePrune is the retention run after each backup, as the prune
// identity.
type servicePrune struct {
	conn   connFlags
	policy prune.Policy
	opts   prune.Options
}

// loadConfig reads and checks the configuration file at path, which must
// be a regular file that group and others cannot write.
func loadConfig(path string) (*serviceConfig, error) {
	f, err := openRestricted(path, othersWrite, errConfigMode)
	if err != nil {
		if errors.Is(err, errConfigMode) {
			return nil, &configError{path: path, err: err}
		}
		return nil, fmt.Errorf("config file %s: %w", path, err)
	}
	b, err := io.ReadAll(io.LimitReader(f, maxConfig+1))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, fmt.Errorf("config file %s: %w", path, pathless(err))
	}
	if len(b) > maxConfig {
		return nil, &configError{path: path, setting: "the file is larger than 1 MiB", err: errConfigInvalid}
	}
	return parseConfig(path, b)
}

func parseConfig(path string, b []byte) (*serviceConfig, error) {
	fail := func(err error, setting string) error {
		return &configError{path: path, setting: setting, err: err}
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var fc fileConfig
	if err := dec.Decode(&fc); err != nil {
		return nil, decodeError(err, fail)
	}
	if dec.More() {
		return nil, fail(errConfigSyntax, "data after the configuration object")
	}
	if fc.Passphrase != nil {
		return nil, fail(errConfigInline, "passphrase (use passphrase_file)")
	}
	c := &serviceConfig{
		repo:      repoFlags{repo: fc.Repo, passFile: fc.PassphraseFile},
		sources:   fc.Sources,
		host:      fc.Host,
		logFile:   fc.LogFile,
		logFormat: fc.LogFormat,
	}
	if err := checkConn(fc.Connection, "connection", &c.conn, fail); err != nil {
		return nil, err
	}
	if fc.PassphraseFile == "" {
		return nil, fail(errConfigMissing, "passphrase_file")
	}
	if len(fc.Sources) == 0 {
		return nil, fail(errConfigMissing, "sources")
	}
	for i, s := range fc.Sources {
		if !filepath.IsAbs(s) {
			return nil, fail(errConfigInvalid, fmt.Sprintf("sources[%d] is not an absolute path", i))
		}
	}
	switch fc.LogFormat {
	case "", "text", "json":
	default:
		return nil, fail(errConfigInvalid, `log_format is not "text" or "json"`)
	}
	sched, err := checkSchedule(fc.Schedule, fail)
	if err != nil {
		return nil, err
	}
	c.schedule = sched
	if fc.Prune != nil {
		if c.prune, err = checkPrune(fc.Prune, fail); err != nil {
			return nil, err
		}
	}
	if c.snap, err = checkSnapshot(fc.SourceSnapshot, fail); err != nil {
		return nil, err
	}
	return c, nil
}

// checkSnapshot checks the source_snapshot object; when it is absent the
// sources are read live.
func checkSnapshot(sc *snapshotConfig, fail func(error, string) error) (srcsnap.Config, error) {
	var c srcsnap.Config
	if sc != nil {
		c = srcsnap.Config{Method: srcsnap.Method(sc.Method), OnFailure: srcsnap.Policy(sc.OnFailure), Dir: sc.Dir}
		if sc.LVM != nil {
			c.LVMSize = sc.LVM.Size
		}
		if sc.Btrfs != nil {
			c.BtrfsDir = sc.Btrfs.Dir
		}
	}
	if err := c.Validate(); err != nil {
		return c, fail(fmt.Errorf("%w: %w", errConfigInvalid, err), "source_snapshot")
	}
	return c, nil
}

// decodeError classifies a decoding error; it carries no setting's value.
func decodeError(err error, fail func(error, string) error) error {
	var (
		syn *json.SyntaxError
		typ *json.UnmarshalTypeError
	)
	switch {
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		return fail(errConfigUnknown, strings.TrimPrefix(err.Error(), "json: unknown field "))
	case errors.As(err, &syn):
		return fail(errConfigSyntax, fmt.Sprintf("at byte %d", syn.Offset))
	case errors.As(err, &typ):
		return fail(errConfigInvalid, typ.Field+" must be a JSON "+typ.Type.Kind().String())
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return fail(errConfigSyntax, "the file ends early")
	}
	return fail(errConfigInvalid, err.Error())
}

func checkConn(cc *connConfig, name string, out *connFlags, fail func(error, string) error) error {
	if cc == nil {
		return fail(errConfigMissing, name)
	}
	if cc.Token != nil {
		return fail(errConfigInline, name+".token (use token_file)")
	}
	if cc.Address == "" {
		return fail(errConfigMissing, name+".address")
	}
	if cc.Protocol == "" {
		cc.Protocol = string(objstore.ProtocolGRPC)
	}
	known := false
	for _, p := range protocols {
		known = known || cc.Protocol == string(p)
	}
	if !known {
		return fail(errConfigInvalid, name+".protocol is not one of rest, grpc, quic, mcp or unix")
	}
	if cc.Timeout < 0 {
		return fail(errConfigInvalid, name+".timeout is negative")
	}
	*out = connFlags{
		prefix: name + ".", protocol: cc.Protocol, address: cc.Address, tls: cc.TLS,
		caFile: cc.CAFile, certFile: cc.CertFile, keyFile: cc.KeyFile,
		tokenFile: cc.TokenFile, tenant: cc.Tenant, timeout: time.Duration(cc.Timeout),
	}
	return nil
}

func checkSchedule(sc *scheduleConfig, fail func(error, string) error) (schedule.Schedule, error) {
	var (
		s   schedule.Schedule
		err error
	)
	switch {
	case sc == nil || (sc.Every == 0 && sc.Daily == ""):
		return s, fail(errConfigMissing, "schedule.every or schedule.daily")
	case sc.Every != 0 && sc.Daily != "":
		return s, fail(errConfigInvalid, "schedule.every and schedule.daily are exclusive")
	case sc.Daily != "":
		if s, err = schedule.Daily(sc.Daily); err != nil {
			return s, fail(errConfigInvalid, "schedule.daily is not HH:MM")
		}
	default:
		if s, err = schedule.Every(time.Duration(sc.Every)); err != nil {
			return s, fail(errConfigInvalid, "schedule.every is shorter than 1m")
		}
	}
	if s, err = s.WithJitter(time.Duration(sc.Jitter)); err != nil {
		return s, fail(errConfigInvalid, "schedule.jitter is negative or not shorter than the schedule's period")
	}
	return s, nil
}

func checkPrune(pc *pruneConfig, fail func(error, string) error) (*servicePrune, error) {
	p := &servicePrune{
		policy: prune.Policy{
			Last: pc.KeepLast, Within: time.Duration(pc.KeepWithin),
			Daily: pc.KeepDaily, Weekly: pc.KeepWeekly, Monthly: pc.KeepMonthly, Yearly: pc.KeepYearly,
		},
		opts: prune.Options{Grace: prune.DefaultGrace},
	}
	if err := checkConn(pc.Connection, "prune.connection", &p.conn, fail); err != nil {
		return nil, err
	}
	if pc.Grace != nil {
		p.opts.Grace = time.Duration(*pc.Grace)
	}
	pol := p.policy
	switch {
	case pol.Last < 0 || pol.Within < 0 || pol.Daily < 0 || pol.Weekly < 0 || pol.Monthly < 0 || pol.Yearly < 0:
		return nil, fail(errConfigInvalid, "a prune.keep_* setting is negative")
	case pol == prune.Policy{}:
		return nil, fail(errConfigMissing, "a prune.keep_* setting: the policy keeps nothing")
	case p.opts.Grace <= 0:
		return nil, fail(errConfigInvalid, "prune.grace is not positive")
	}
	return p, nil
}
