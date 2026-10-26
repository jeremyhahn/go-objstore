package srcsnap

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
	"strings"
)

// zfsDatasetRe is the dataset names the driver accepts.
var zfsDatasetRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/-]*$`)

// zfsDriver takes a ZFS snapshot of the dataset that holds a source, reads
// it through the dataset's .zfs/snapshot directory, and destroys it on
// cleanup.
type zfsDriver struct{ s *Snapshotter }

func (d *zfsDriver) base(v Volume) string { return v.MountPoint }

func (d *zfsDriver) create(ctx context.Context, v Volume, name string, t *Set) (string, error) {
	s := d.s
	if !ValidName(name) {
		return "", &Error{Op: opCreate, Method: ZFS, Volume: v.Device, Snapshot: name, Err: ErrName}
	}
	if !zfsDatasetRe.MatchString(v.Device) {
		return "", &Error{Op: opCreate, Method: ZFS, Volume: v.Device, Err: ErrUnsupported,
			Cause: detail("%q is not a ZFS dataset name", v.Device)}
	}
	snap := v.Device + "@" + name
	if _, err := s.run(ctx, ZFS, opCreate, v.Device, name, "zfs", "snapshot", snap); err != nil {
		return "", err
	}
	s.logStep("source snapshot created", ZFS, v.Device, name)
	t.push(func(ctx context.Context) error {
		if _, err := s.run(ctx, ZFS, "remove", v.Device, name, "zfs", "destroy", snap); err != nil {
			return err
		}
		s.logStep("source snapshot removed", ZFS, v.Device, name)
		return nil
	})
	return filepath.Join(v.MountPoint, ".zfs", "snapshot", name), nil
}

// sweep destroys every leftover snapshot with the name prefix.
func (d *zfsDriver) sweep(ctx context.Context, _ []Volume) error {
	s := d.s
	out, err := s.run(ctx, ZFS, "list snapshots", "", "", "zfs", "list", "-H", "-t", "snapshot", "-o", "name")
	if err != nil {
		return err
	}
	var errs []error
	for _, line := range strings.Split(string(out), "\n") {
		ds, name, ok := strings.Cut(strings.TrimSpace(line), "@")
		if !ok || !ValidName(name) || !zfsDatasetRe.MatchString(ds) {
			continue
		}
		if _, err := s.run(ctx, ZFS, "remove leftover", ds, name, "zfs", "destroy", ds+"@"+name); err != nil {
			errs = append(errs, err)
			continue
		}
		s.logStep("source snapshot leftover removed", ZFS, ds, name)
	}
	return errors.Join(errs...)
}
