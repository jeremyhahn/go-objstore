package main

import (
	"context"
	"flag"
	"log/slog"

	"github.com/jeremyhahn/go-objstore/backup/pkg/srcsnap"
)

// snapHelp describes source snapshots for the backup command's help.
const snapHelp = `
--snapshot reads each path from a crash-consistent snapshot of its volume,
taken for the run and removed after it; the backup still records the
original path. It needs the privileges of the snapshot tool, usually root or
Administrator.
`

// snapOpts are the source snapshot flags.
type snapOpts struct {
	method, onFailure, dir, lvmSize, btrfsDir string
}

func (o *snapOpts) register(fs *flag.FlagSet) {
	fs.StringVar(&o.method, "snapshot", "", "source snapshot method: none, auto, lvm, btrfs, zfs, apfs or vss (default none)")
	fs.StringVar(&o.onFailure, "snapshot-on-failure", "",
		`when the snapshot cannot be made: "fail" the backup (default) or "fallback" to the live sources`)
	fs.StringVar(&o.dir, "snapshot-dir", "", "directory for snapshot mount points, records and the lock (default "+srcsnap.DefaultDir()+")")
	fs.StringVar(&o.lvmSize, "snapshot-lvm-size", "", "LVM snapshot size, such as 4G or 20%ORIGIN (default "+srcsnap.DefaultLVMSize+")")
	fs.StringVar(&o.btrfsDir, "snapshot-btrfs-dir", "", "directory btrfs snapshots are made in (default: beside each subvolume)")
}

// config returns the checked configuration the flags name.
func (o *snapOpts) config() (srcsnap.Config, error) {
	c := srcsnap.Config{
		Method:    srcsnap.Method(o.method),
		OnFailure: srcsnap.Policy(o.onFailure),
		Dir:       o.dir,
		LVMSize:   o.lvmSize,
		BtrfsDir:  o.btrfsDir,
	}
	err := c.Validate()
	return c, err
}

// snapshotter returns a Snapshotter for cfg that logs to log.
func (e *env) snapshotter(cfg srcsnap.Config, log *slog.Logger) (*srcsnap.Snapshotter, error) {
	se := e.snap
	se.Log = log
	return srcsnap.New(cfg, se)
}

// snapshot sweeps the leftovers of earlier runs, then snapshots sources.
// A failed sweep is logged and left to Create. Under the fallback policy a
// failed snapshot is logged and the live sources are read: the Set is nil.
func snapshot(ctx context.Context, s *srcsnap.Snapshotter, sources []string, log *slog.Logger) (*srcsnap.Set, error) {
	cfg := s.Config()
	if cfg.Method == srcsnap.None {
		return nil, nil
	}
	if err := s.Sweep(ctx, sources); err != nil {
		log.Warn("source snapshot sweep failed", "method", string(cfg.Method), "error", err.Error())
	}
	set, err := s.Create(ctx, sources)
	if err != nil && cfg.OnFailure == srcsnap.Fallback && ctx.Err() == nil {
		log.Warn("source snapshot failed; backing up the live sources", "method", string(cfg.Method), "error", err.Error())
		return nil, nil
	}
	return set, err
}
