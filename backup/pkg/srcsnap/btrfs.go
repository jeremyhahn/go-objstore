package srcsnap

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// btrfsDriver takes a read-only snapshot of the subvolume that holds a
// source, beside the subvolume or in Config.BtrfsDir, and deletes it on
// cleanup.
type btrfsDriver struct{ s *Snapshotter }

func (d *btrfsDriver) base(v Volume) string {
	if v.Subvolume != "" {
		return v.Subvolume
	}
	return v.MountPoint
}

// BtrfsHiddenDir is the directory, at the top of a btrfs file system whose
// subvolume is mounted at its own root, that holds a run's snapshots by
// default: the mount point's parent is another file system, which cannot
// hold them. A run creates it and removes it, and a sweep removes it with
// the leftovers inside. Its leading dot keeps it from being taken for a
// snapshot.
const BtrfsHiddenDir = "." + NamePrefix + "snapshots"

// dir returns where v's snapshots are made, and whether that is the hidden
// directory the run creates.
func (d *btrfsDriver) dir(v Volume) (string, bool) {
	if d.s.cfg.BtrfsDir != "" {
		return d.s.cfg.BtrfsDir, false
	}
	base := d.base(v)
	if base == v.MountPoint {
		return filepath.Join(base, BtrfsHiddenDir), true
	}
	return filepath.Dir(base), false
}

// makeHidden creates the hidden directory dir, or takes a leftover one, and
// refuses anything there that is not a directory, such as a symbolic link.
func makeHidden(dir string) error {
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return detail("%s is not a directory", dir)
	}
	return nil
}

// removeHidden removes the hidden directory dir; one already gone is not an
// error.
func removeHidden(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// A directory that still holds something, another volume's snapshot of
	// the same file system or a file this package did not make, stays.
	if len(entries) > 0 {
		return nil
	}
	if err := os.Remove(dir); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func (d *btrfsDriver) create(ctx context.Context, v Volume, name string, t *Set) (string, error) {
	s := d.s
	subvol := d.base(v)
	if !ValidName(name) {
		return "", &Error{Op: opCreate, Method: Btrfs, Volume: subvol, Snapshot: name, Err: ErrName}
	}
	dir, hidden := d.dir(v)
	snap := filepath.Join(dir, name)
	if hidden {
		if err := makeHidden(dir); err != nil {
			return "", &Error{Op: opCreate, Method: Btrfs, Volume: subvol, Snapshot: name, Err: ErrFilesystem, Cause: err}
		}
		t.push(func(context.Context) error {
			if err := removeHidden(dir); err != nil {
				return &Error{Op: "remove", Method: Btrfs, Volume: subvol, Snapshot: name, Err: ErrFilesystem, Cause: err}
			}
			return nil
		})
		// The hidden directory is inside the source's own file system, so
		// a backup of the source, from the snapshot or live, must skip it.
		t.skip = append(t.skip, dir)
	} else {
		t.skip = append(t.skip, snap)
	}
	if _, err := s.run(ctx, Btrfs, opCreate, subvol, name, "btrfs", "subvolume", "snapshot", "-r", subvol, snap); err != nil {
		return "", err
	}
	s.logStep("source snapshot created", Btrfs, subvol, name)
	t.push(func(ctx context.Context) error {
		if _, err := s.run(ctx, Btrfs, "remove", subvol, name, "btrfs", "subvolume", "delete", snap); err != nil {
			return err
		}
		s.logStep("source snapshot removed", Btrfs, subvol, name)
		return nil
	})
	return snap, nil
}

// sweep deletes leftover snapshots in the directories the sources'
// snapshots are made in.
func (d *btrfsDriver) sweep(ctx context.Context, vols []Volume) error {
	s := d.s
	seen := map[string]bool{}
	var errs []error
	for _, v := range vols {
		dir, hidden := d.dir(v)
		if seen[dir] {
			continue
		}
		seen[dir] = true
		entries, err := os.ReadDir(dir)
		if hidden && errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, &Error{Op: opSweep, Method: Btrfs, Volume: dir, Err: ErrFilesystem, Cause: err})
			continue
		}
		failed := false
		for _, e := range entries {
			if !e.IsDir() || !ValidName(e.Name()) {
				continue
			}
			snap := filepath.Join(dir, e.Name())
			if _, err := s.run(ctx, Btrfs, "remove leftover", dir, e.Name(), "btrfs", "subvolume", "delete", snap); err != nil {
				errs = append(errs, err)
				failed = true
				continue
			}
			s.logStep("source snapshot leftover removed", Btrfs, dir, e.Name())
		}
		if hidden && !failed {
			if err := removeHidden(dir); err != nil {
				errs = append(errs, &Error{Op: opSweep, Method: Btrfs, Volume: dir, Err: ErrFilesystem, Cause: err})
			}
		}
	}
	return errors.Join(errs...)
}
