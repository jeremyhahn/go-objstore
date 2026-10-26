package srcsnap

import (
	"context"
	"errors"
	"regexp"
	"sort"
)

var (
	// tmutilDateRe finds the date tmutil localsnapshot names its snapshot
	// by.
	tmutilDateRe = regexp.MustCompile(`\b([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{6})\b`)
	apfsDateRe   = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{6}$`)
	apfsDeviceRe = regexp.MustCompile(`^/dev/disk[0-9]+(s[0-9]+)*$`)
)

// apfsDriver takes one local snapshot with tmutil localsnapshot, which
// covers every local APFS volume at once, mounts it read-only with
// mount_apfs -s for each source volume, and on cleanup unmounts it and
// deletes it with tmutil deletelocalsnapshots. tmutil names the snapshot
// by its date, so the driver records the date in a state file named with
// the prefix; Sweep deletes only snapshots a state file records.
type apfsDriver struct {
	s    *Snapshotter
	date string
	// names are the state files written for the snapshot.
	names []string
}

func (d *apfsDriver) base(v Volume) string { return v.MountPoint }

func (d *apfsDriver) create(ctx context.Context, v Volume, name string, t *Set) (string, error) {
	s := d.s
	if !ValidName(name) {
		return "", &Error{Op: opCreate, Method: APFS, Volume: v.Device, Snapshot: name, Err: ErrName}
	}
	if !apfsDeviceRe.MatchString(v.Device) {
		return "", &Error{Op: opCreate, Method: APFS, Volume: v.Device, Err: ErrUnsupported,
			Cause: detail("%q is not an APFS volume device", v.Device)}
	}
	if d.date == "" {
		out, err := s.run(ctx, APFS, opCreate, v.Device, name, "tmutil", "localsnapshot")
		if err != nil {
			return "", err
		}
		m := tmutilDateRe.FindSubmatch(out)
		if m == nil {
			return "", &Error{Op: opCreate, Method: APFS, Volume: v.Device, Snapshot: name, Err: ErrCommand,
				Cause: detail("tmutil printed no snapshot date")}
		}
		d.date = string(m[1])
		s.logStep("source snapshot created", APFS, v.Device, apfsSnapshot(d.date))
		date := d.date
		t.push(func(ctx context.Context) error {
			if err := d.s.deleteAPFS(ctx, date); err != nil {
				return err
			}
			var errs []error
			for _, n := range d.names {
				errs = append(errs, s.removeState(APFS, n))
			}
			return errors.Join(errs...)
		})
	}
	if err := s.writeState(APFS, v.Device, name, d.date); err != nil {
		return "", err
	}
	d.names = append(d.names, name)
	mnt, err := s.mountPoint(APFS, v.Device, name, t)
	if err != nil {
		return "", err
	}
	if _, err := s.run(ctx, APFS, "mount", v.Device, name,
		"mount_apfs", "-o", "rdonly,nobrowse", "-s", apfsSnapshot(d.date), v.Device, mnt); err != nil {
		return "", err
	}
	s.logStep("source snapshot mounted", APFS, v.Device, name)
	t.push(func(ctx context.Context) error { return s.unmount(ctx, APFS, v.Device, name, mnt) })
	return mnt, nil
}

// apfsSnapshot is the name tmutil gives the snapshot of date.
func apfsSnapshot(date string) string { return "com.apple.TimeMachine." + date + ".local" }

// deleteAPFS deletes the local snapshot of date.
func (s *Snapshotter) deleteAPFS(ctx context.Context, date string) error {
	if _, err := s.run(ctx, APFS, "remove", "", apfsSnapshot(date), "tmutil", "deletelocalsnapshots", date); err != nil {
		return err
	}
	s.logStep("source snapshot removed", APFS, "", apfsSnapshot(date))
	return nil
}

// sweep deletes the snapshots that leftover state files record, then the
// state files; their mount points went first.
func (d *apfsDriver) sweep(ctx context.Context, _ []Volume) error {
	s := d.s
	states, err := s.states(APFS)
	if err != nil {
		return err
	}
	var errs []error
	byDate := map[string][]string{}
	for name, date := range states {
		if !apfsDateRe.MatchString(date) {
			errs = append(errs, &Error{Op: opSweep, Method: APFS, Snapshot: name, Err: ErrName,
				Cause: detail("the state file holds no snapshot date")})
			continue
		}
		byDate[date] = append(byDate[date], name)
	}
	dates := make([]string, 0, len(byDate))
	for date := range byDate {
		dates = append(dates, date)
	}
	sort.Strings(dates)
	for _, date := range dates {
		if _, err := s.run(ctx, APFS, "remove leftover", "", apfsSnapshot(date), "tmutil", "deletelocalsnapshots", date); err != nil {
			errs = append(errs, err)
			continue
		}
		s.logStep("source snapshot leftover removed", APFS, "", apfsSnapshot(date))
		for _, name := range byDate[date] {
			errs = append(errs, s.removeState(APFS, name))
		}
	}
	return errors.Join(errs...)
}
