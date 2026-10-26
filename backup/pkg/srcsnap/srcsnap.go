// Package srcsnap takes a point-in-time snapshot of the volumes a backup's
// sources live on, so a backup reads a frozen view instead of a live,
// changing file system.
//
// A Snapshotter snapshots each volume once, through the platform's own
// tools (LVM, btrfs, ZFS, APFS through tmutil and mount_apfs, or Windows
// VSS through PowerShell's CIM cmdlets), run with an argument vector and
// never a shell. Create returns a Set that maps each source to the same
// directory inside its snapshot; Set.Close removes everything Create made,
// and runs even when the context is canceled. Snapshot names carry the
// prefix NamePrefix and a random suffix, and Sweep removes leftovers with
// that prefix from a run that crashed, never anything else.
//
// There is no fsfreeze method: freezing a live file system for the length
// of a walk would stall every writer on it.
package srcsnap

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
)

// Method names how a source volume is snapshotted.
type Method string

// The methods. None takes no snapshot; Auto picks one per volume by its
// file system.
const (
	None  Method = "none"
	Auto  Method = "auto"
	LVM   Method = "lvm"
	Btrfs Method = "btrfs"
	ZFS   Method = "zfs"
	APFS  Method = "apfs"
	VSS   Method = "vss"
)

// Methods lists every method in the order the documentation gives them.
var Methods = []Method{None, Auto, LVM, Btrfs, ZFS, APFS, VSS}

// ParseMethod parses a method name; empty means None.
func ParseMethod(s string) (Method, error) {
	if s == "" {
		return None, nil
	}
	for _, m := range Methods {
		if string(m) == s {
			return m, nil
		}
	}
	return "", &Error{Op: "parse method", Err: ErrConfig, Cause: detail("unknown method %q", s)}
}

// Policy says what a run does when the snapshot cannot be made.
type Policy string

const (
	// Fail fails the run. It is the default.
	Fail Policy = "fail"
	// Fallback backs up the live source and logs a warning.
	Fallback Policy = "fallback"
)

// ParsePolicy parses a failure policy; empty means Fail.
func ParsePolicy(s string) (Policy, error) {
	switch Policy(s) {
	case "", Fail:
		return Fail, nil
	case Fallback:
		return Fallback, nil
	}
	return "", &Error{Op: "parse policy", Err: ErrConfig, Cause: detail("unknown failure policy %q", s)}
}

// DefaultLVMSize is the LVM snapshot size when none is set: copy-on-write
// space of a tenth of the origin volume.
const DefaultLVMSize = "10%ORIGIN"

// CleanupTimeout bounds the removal of a snapshot, which runs on a context
// of its own so a canceled run still cleans up.
const CleanupTimeout = 2 * time.Minute

// Config is a source snapshot configuration.
type Config struct {
	// Method is the snapshot method; empty means None.
	Method Method
	// OnFailure is the failure policy; empty means Fail. The package only
	// carries it; the caller applies it.
	OnFailure Policy
	// Dir holds the mount points (LVM, APFS), the state files (APFS, VSS)
	// and the lock. Empty means DefaultDir. It must be a directory only the
	// agent's user can write.
	Dir string
	// LVMSize bounds an LVM snapshot's copy-on-write space: a size such as
	// "4G" (lvcreate --size) or a percentage of 1 to 100 of ORIGIN, FREE or
	// VG such as "10%ORIGIN" (lvcreate --extents). Empty means
	// DefaultLVMSize.
	LVMSize string
	// BtrfsDir is the directory btrfs snapshots are created in, on the
	// same file system as each subvolume. Empty means the subvolume's
	// parent directory.
	BtrfsDir string
}

var (
	lvmSizeRe    = regexp.MustCompile(`^[1-9][0-9]{0,8}(\.[0-9]{1,3})?[kKmMgGtTpP]$`)
	lvmExtentsRe = regexp.MustCompile(`^([1-9][0-9]?|100)%(ORIGIN|FREE|VG)$`)
)

// Validate checks the configuration and fills in its defaults.
func (c *Config) Validate() error {
	fail := func(format string, a ...any) error {
		return &Error{Op: "check configuration", Method: c.Method, Err: ErrConfig, Cause: detail(format, a...)}
	}
	m, err := ParseMethod(string(c.Method))
	if err != nil {
		return err
	}
	c.Method = m
	p, err := ParsePolicy(string(c.OnFailure))
	if err != nil {
		return err
	}
	c.OnFailure = p
	if c.Dir == "" {
		c.Dir = DefaultDir()
	}
	if !filepath.IsAbs(c.Dir) {
		return fail("the snapshot directory is not an absolute path")
	}
	c.Dir = filepath.Clean(c.Dir)
	if c.LVMSize != "" && c.Method != LVM && c.Method != Auto {
		return fail("an LVM size is set but the method is %s", c.Method)
	}
	if c.LVMSize == "" && (c.Method == LVM || c.Method == Auto) {
		c.LVMSize = DefaultLVMSize
	}
	if c.LVMSize != "" && !lvmSizeRe.MatchString(c.LVMSize) && !lvmExtentsRe.MatchString(c.LVMSize) {
		return fail("the LVM size is not a size such as 4G or a percentage such as 10%%ORIGIN")
	}
	if c.BtrfsDir != "" {
		if c.Method != Btrfs && c.Method != Auto {
			return fail("a btrfs directory is set but the method is %s", c.Method)
		}
		if !filepath.IsAbs(c.BtrfsDir) {
			return fail("the btrfs directory is not an absolute path")
		}
		c.BtrfsDir = filepath.Clean(c.BtrfsDir)
	}
	return nil
}

// lvmSizeArgs returns lvcreate's option for the configured size.
func (c *Config) lvmSizeArgs() []string {
	if lvmExtentsRe.MatchString(c.LVMSize) {
		return []string{"--extents", c.LVMSize}
	}
	return []string{"--size", c.LVMSize}
}

// NamePrefix starts every snapshot name the package makes. A name is the
// prefix and 16 lowercase hexadecimal digits.
const NamePrefix = "objstore-backup-"

var nameRe = regexp.MustCompile(`^` + NamePrefix + `[0-9a-f]{16}$`)

// ValidName reports whether name is a snapshot name the package makes.
// Nothing is created, mounted or removed under a name that fails it.
func ValidName(name string) bool { return nameRe.MatchString(name) }

// Volume is the volume a path lives on, as a Locator reports it.
type Volume struct {
	// MountPoint is where the file system is mounted (on Windows, the
	// volume's root path, such as `C:\`).
	MountPoint string
	// Root is the directory of the file system mounted at MountPoint: "/"
	// unless it is a bind or subvolume mount.
	Root string
	// FSType is the file system type, such as "ext4", "btrfs" or "NTFS".
	FSType string
	// Device is the mount source: a block device, a ZFS dataset, or the
	// volume path on Windows.
	Device string
	// Subvolume is, on btrfs, the root directory of the subvolume that
	// holds the path.
	Subvolume string
}

// Locator finds the volume a path lives on.
type Locator interface {
	// Locate returns the volume path lives on and path resolved to the
	// form the volume's MountPoint (or Subvolume) is a prefix of.
	Locate(path string) (Volume, string, error)
	// MountPoints returns the mount points in use.
	MountPoints() (map[string]bool, error)
}

// Env holds the Snapshotter's dependencies. Every field is optional.
type Env struct {
	// Runner runs the tools; nil runs them with os/exec.
	Runner Runner
	// Locator finds volumes; nil uses the platform's.
	Locator Locator
	// LookPath resolves a tool name to an absolute path; nil searches the
	// system's tool directories, never $PATH.
	LookPath func(tool string) (string, error)
	// Log receives a record for each step; nil discards them.
	Log *slog.Logger
	// Rand supplies snapshot name suffixes; nil uses crypto/rand.
	Rand io.Reader
	// HostDir holds the host lock; "" uses the platform's fixed directory,
	// the same for every run on the host. Only tests set it.
	HostDir string
}

// Snapshotter takes source snapshots under one configuration. Tools are
// resolved once, on first use, and kept.
type Snapshotter struct {
	cfg Config
	env Env

	mu    sync.Mutex
	tools map[string]string
}

// New returns a Snapshotter for cfg, which it validates.
func New(cfg Config, env Env) (*Snapshotter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if env.Runner == nil {
		env.Runner = ExecRunner{}
	}
	if env.Locator == nil {
		env.Locator = platformLocator{}
	}
	if env.LookPath == nil {
		env.LookPath = lookTool
	}
	if env.Log == nil {
		env.Log = slog.New(slog.DiscardHandler)
	}
	if env.Rand == nil {
		env.Rand = rand.Reader
	}
	if env.HostDir == "" {
		env.HostDir = hostLockDir()
	}
	return &Snapshotter{cfg: cfg, env: env, tools: map[string]string{}}, nil
}

// Config returns the validated configuration.
func (s *Snapshotter) Config() Config { return s.cfg }

// Set is the snapshot of a run's sources. A nil Set reads the live
// sources: Path returns its argument and Close does nothing.
type Set struct {
	s     *Snapshotter
	paths map[string]string
	skip  []string
	undo  []func(context.Context) error
	once  sync.Once
	err   error
}

// Path returns the path to read for source p, one of the paths given to
// Create, inside its snapshot. A path Create was not given is returned
// as it is.
func (t *Set) Path(p string) string {
	if t == nil {
		return p
	}
	if r, ok := t.paths[p]; ok {
		return r
	}
	return p
}

// Skip returns the directories, by their paths on the live file systems,
// that hold the set's snapshots inside a file system being backed up. A
// backup must not walk them: it would read a snapshot, its own or another
// source's, as part of the tree. Nil Sets skip nothing.
func (t *Set) Skip() []string {
	if t == nil {
		return nil
	}
	return slices.Clone(t.skip)
}

// Close removes what Create made, in reverse order, on a context of its
// own bounded by CleanupTimeout, so it runs after the run's context is
// canceled. Every step runs even if an earlier one fails. Close is safe to
// call more than once and returns the first call's result.
func (t *Set) Close() error {
	if t == nil {
		return nil
	}
	t.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), CleanupTimeout)
		defer cancel()
		var errs []error
		for i := len(t.undo) - 1; i >= 0; i-- {
			errs = append(errs, t.undo[i](ctx))
		}
		t.undo = nil
		t.err = errors.Join(errs...)
	})
	return t.err
}

func (t *Set) push(f func(context.Context) error) { t.undo = append(t.undo, f) }

// driver is one method's implementation. A driver value lives for one
// Create or Sweep.
type driver interface {
	// base returns the directory of v whose snapshot create returns.
	base(v Volume) string
	// create snapshots v under name and returns the directory inside the
	// snapshot that corresponds to base(v). Each step it completes pushes
	// its undo onto t, so a failure part way removes only what was made.
	create(ctx context.Context, v Volume, name string, t *Set) (string, error)
	// sweep removes leftovers with the name prefix; vols are the volumes
	// of the sources under this method.
	sweep(ctx context.Context, vols []Volume) error
}

func (s *Snapshotter) driver(m Method) driver {
	switch m {
	case LVM:
		return &lvmDriver{s: s}
	case Btrfs:
		return &btrfsDriver{s: s}
	case ZFS:
		return &zfsDriver{s: s}
	case APFS:
		return &apfsDriver{s: s}
	default:
		return &vssDriver{s: s}
	}
}

// pick chooses a method for a volume by its file system.
func pick(v Volume) (Method, bool) {
	switch strings.ToLower(v.FSType) {
	case "btrfs":
		return Btrfs, true
	case "zfs":
		return ZFS, true
	case "apfs":
		return APFS, true
	case "ntfs", "refs":
		return VSS, true
	case "ext2", "ext3", "ext4", "xfs":
		// Only a device-mapper device can be a logical volume.
		if strings.HasPrefix(v.Device, "/dev/mapper/") || strings.HasPrefix(v.Device, "/dev/dm-") {
			return LVM, true
		}
	}
	return "", false
}

// source is one source path located on its volume.
type source struct {
	path     string
	method   Method
	vol      Volume
	resolved string
}

func (s *Snapshotter) locate(paths []string) ([]source, error) {
	out := make([]source, 0, len(paths))
	for _, p := range paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			return nil, &Error{Op: opLocate, Volume: p, Err: ErrFilesystem, Cause: err}
		}
		v, resolved, err := s.env.Locator.Locate(abs)
		if err != nil {
			return nil, &Error{Op: opLocate, Volume: abs, Err: ErrFilesystem, Cause: err}
		}
		m := s.cfg.Method
		if m == Auto {
			var ok bool
			if m, ok = pick(v); !ok {
				return nil, &Error{Op: opLocate, Method: Auto, Volume: v.MountPoint, Err: ErrUnsupported,
					Cause: detail("no method for file system %q on %s", v.FSType, v.Device)}
			}
		}
		if m == ZFS && v.Root != "" && v.Root != "/" {
			// A ZFS snapshot is reached under .zfs at the dataset's own
			// mount point, which a bind mount of a directory in it lacks.
			return nil, &Error{Op: opLocate, Method: ZFS, Volume: v.MountPoint, Err: ErrUnsupported,
				Cause: detail("%s is a bind mount of %s in dataset %s; name the source under the dataset's own mount point",
					v.MountPoint, v.Root, v.Device)}
		}
		out = append(out, source{path: p, method: m, vol: v, resolved: resolved})
	}
	return out, nil
}

// Create snapshots the volume of every path in paths, each volume once,
// and returns the Set mapping each path into its snapshot. With method
// None it returns a nil Set. On failure everything already made is removed
// before Create returns. The host lock and the snapshot directory stay
// locked until Close.
func (s *Snapshotter) Create(ctx context.Context, paths []string) (*Set, error) {
	if s.cfg.Method == None {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, &Error{Op: opCreate, Method: s.cfg.Method, Err: ErrCanceled, Cause: err}
	}
	t := &Set{s: s, paths: map[string]string{}}
	unlock, err := s.lock()
	if err != nil {
		return nil, err
	}
	t.push(func(context.Context) error { return unlock() })
	if err := s.create(ctx, paths, t); err != nil {
		return nil, errors.Join(err, t.Close())
	}
	return t, nil
}

func (s *Snapshotter) create(ctx context.Context, paths []string, t *Set) error {
	srcs, err := s.locate(paths)
	if err != nil {
		return err
	}
	drivers := map[Method]driver{}
	roots := map[string]string{}
	for i := range srcs {
		src := &srcs[i]
		d, ok := drivers[src.method]
		if !ok {
			d = s.driver(src.method)
			drivers[src.method] = d
		}
		base := d.base(src.vol)
		rel, err := filepath.Rel(base, src.resolved)
		if err != nil || !within(base, src.resolved) {
			return &Error{Op: opLocate, Method: src.method, Volume: base, Err: ErrUnsupported,
				Cause: detail("%s is not inside %s", src.resolved, base)}
		}
		key := string(src.method) + "\x00" + base
		root, ok := roots[key]
		if !ok {
			if err := ctx.Err(); err != nil {
				return &Error{Op: opCreate, Method: src.method, Volume: base, Err: ErrCanceled, Cause: err}
			}
			name, err := s.newName()
			if err != nil {
				return err
			}
			if root, err = d.create(ctx, src.vol, name, t); err != nil {
				return err
			}
			roots[key] = root
		}
		if rel == "." {
			t.paths[src.path] = root
		} else {
			t.paths[src.path] = filepath.Join(root, rel)
		}
	}
	return nil
}

// Sweep removes the leftovers of runs that crashed: snapshots, mount
// points and state files whose names carry NamePrefix, for the methods the
// paths' volumes use. It never removes a name without the prefix. Each
// removal is logged. With method None it does nothing.
//
// The ZFS and LVM sweeps remove prefixed snapshots anywhere on the host, and
// the btrfs sweep those in the volumes' snapshot directories, which runs
// with other snapshot directories share. Sweep, like Create, takes the host
// lock, which every run takes whatever its directory, so it never runs
// beside a live run, and fails as busy if one is.
func (s *Snapshotter) Sweep(ctx context.Context, paths []string) (err error) {
	if s.cfg.Method == None {
		return nil
	}
	unlock, err := s.lock()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, unlock()) }()
	errs := []error{s.sweepMounts(ctx)}
	methods := []Method{s.cfg.Method}
	vols := map[Method][]Volume{}
	srcs, err := s.locate(paths)
	if err != nil {
		errs = append(errs, err)
	}
	if s.cfg.Method == Auto {
		methods = nil
	}
	for i := range srcs {
		src := &srcs[i]
		if _, seen := vols[src.method]; !seen && s.cfg.Method == Auto {
			methods = append(methods, src.method)
		}
		vols[src.method] = append(vols[src.method], src.vol)
	}
	for _, m := range methods {
		if err := ctx.Err(); err != nil {
			errs = append(errs, &Error{Op: opSweep, Method: m, Err: ErrCanceled, Cause: err})
			break
		}
		errs = append(errs, s.driver(m).sweep(ctx, vols[m]))
	}
	return errors.Join(errs...)
}

// hostLockName is the host lock file in the host lock directory.
const hostLockName = ".host.lock"

// opLocate is the Op of an Error from finding a source's volume and method.
const opLocate = "locate"

// lock takes the host lock, then the lock of the snapshot directory, each
// without waiting. Every run takes the host lock whatever its snapshot
// directory, so a sweep holding it knows no live run anywhere on the host
// owns a snapshot with the name prefix.
func (s *Snapshotter) lock() (func() error, error) {
	unlockHost, err := lockDir(s.env.HostDir, hostLockName)
	if err != nil {
		return nil, err
	}
	unlock, err := lockDir(s.cfg.Dir, lockName)
	if err != nil {
		return nil, errors.Join(err, unlockHost())
	}
	return func() error { return errors.Join(unlock(), unlockHost()) }, nil
}

// sweepMounts unmounts and removes leftover mount points in the snapshot
// directory.
func (s *Snapshotter) sweepMounts(ctx context.Context) error {
	entries, err := os.ReadDir(s.cfg.Dir)
	if err != nil {
		return &Error{Op: opSweep, Volume: s.cfg.Dir, Err: ErrFilesystem, Cause: err}
	}
	var mounts map[string]bool
	var errs []error
	for _, e := range entries {
		if !e.IsDir() || !ValidName(e.Name()) {
			continue
		}
		if mounts == nil {
			if mounts, err = s.env.Locator.MountPoints(); err != nil {
				return &Error{Op: opSweep, Volume: s.cfg.Dir, Err: ErrFilesystem, Cause: err}
			}
		}
		mnt := filepath.Join(s.cfg.Dir, e.Name())
		if mounts[mnt] {
			if _, err := s.run(ctx, "", "unmount leftover", mnt, e.Name(), "umount", mnt); err != nil {
				errs = append(errs, err)
				continue
			}
			s.logStep("source snapshot leftover unmounted", "", mnt, e.Name())
		}
		if err := os.Remove(mnt); err != nil {
			errs = append(errs, &Error{Op: opSweep, Volume: mnt, Snapshot: e.Name(), Err: ErrFilesystem, Cause: err})
		}
	}
	return errors.Join(errs...)
}

// newName returns a fresh snapshot name.
func (s *Snapshotter) newName() (string, error) {
	var b [8]byte
	if _, err := io.ReadFull(s.env.Rand, b[:]); err != nil {
		return "", &Error{Op: "name", Err: ErrName, Cause: err}
	}
	name := NamePrefix + hex.EncodeToString(b[:])
	if !ValidName(name) {
		return "", &Error{Op: "name", Snapshot: name, Err: ErrName}
	}
	return name, nil
}

// mountPoint makes the mount point for name in the snapshot directory and
// pushes its removal.
func (s *Snapshotter) mountPoint(m Method, volume, name string, t *Set) (string, error) {
	if !ValidName(name) {
		return "", &Error{Op: "mount", Method: m, Volume: volume, Snapshot: name, Err: ErrName}
	}
	mnt := filepath.Join(s.cfg.Dir, name)
	if err := os.Mkdir(mnt, 0o700); err != nil {
		return "", &Error{Op: "mount", Method: m, Volume: volume, Snapshot: name, Err: ErrFilesystem, Cause: err}
	}
	t.push(func(context.Context) error {
		if err := os.Remove(mnt); err != nil {
			return &Error{Op: "remove mount point", Method: m, Volume: volume, Snapshot: name, Err: ErrFilesystem, Cause: err}
		}
		return nil
	})
	return mnt, nil
}

// unmount unmounts mnt and logs it.
func (s *Snapshotter) unmount(ctx context.Context, m Method, volume, name, mnt string) error {
	if _, err := s.run(ctx, m, "unmount", volume, name, "umount", mnt); err != nil {
		return err
	}
	s.logStep("source snapshot unmounted", m, volume, name)
	return nil
}

// stateFile is the path of name's state file, which records what a tool
// named for a snapshot whose name the package cannot choose.
func (s *Snapshotter) stateFile(name string, m Method) string {
	return filepath.Join(s.cfg.Dir, name+"."+string(m))
}

// writeState records value in name's state file.
func (s *Snapshotter) writeState(m Method, volume, name, value string) error {
	if !ValidName(name) {
		return &Error{Op: "record", Method: m, Volume: volume, Snapshot: name, Err: ErrName}
	}
	if err := os.WriteFile(s.stateFile(name, m), []byte(value+"\n"), 0o600); err != nil {
		return &Error{Op: "record", Method: m, Volume: volume, Snapshot: name, Err: ErrFilesystem, Cause: err}
	}
	return nil
}

// removeState removes name's state file.
func (s *Snapshotter) removeState(m Method, name string) error {
	if err := os.Remove(s.stateFile(name, m)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return &Error{Op: "remove record", Method: m, Snapshot: name, Err: ErrFilesystem, Cause: err}
	}
	return nil
}

// states returns the names and contents of m's state files in the
// snapshot directory whose names carry the prefix.
func (s *Snapshotter) states(m Method) (map[string]string, error) {
	entries, err := os.ReadDir(s.cfg.Dir)
	if err != nil {
		return nil, &Error{Op: opSweep, Method: m, Volume: s.cfg.Dir, Err: ErrFilesystem, Cause: err}
	}
	out := map[string]string{}
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), "."+string(m))
		if !ok || !ValidName(name) || !e.Type().IsRegular() {
			continue
		}
		b, err := readRecord(filepath.Join(s.cfg.Dir, e.Name()))
		if err != nil {
			return nil, &Error{Op: opSweep, Method: m, Snapshot: name, Err: ErrFilesystem, Cause: err}
		}
		out[name] = strings.TrimSpace(string(b))
	}
	return out, nil
}

func (s *Snapshotter) logStep(msg string, m Method, volume, name string) {
	s.env.Log.Info(msg, "method", string(m), "volume", volume, "snapshot", name)
}
