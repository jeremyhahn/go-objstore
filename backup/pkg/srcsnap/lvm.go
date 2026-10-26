package srcsnap

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
	"strings"
)

// lvmNameRe is LVM's rule for volume group and logical volume names.
var (
	lvmNameRe = regexp.MustCompile(`^[A-Za-z0-9+_.][A-Za-z0-9+_.-]*$`)
	fsTypeRe  = regexp.MustCompile(`^[a-z0-9]+$`)
)

// lvmDriver snapshots a logical volume with lvcreate --snapshot, mounts it
// read-only under the snapshot directory, and on cleanup unmounts it and
// removes it with lvremove.
type lvmDriver struct{ s *Snapshotter }

func (d *lvmDriver) base(v Volume) string { return v.MountPoint }

func (d *lvmDriver) create(ctx context.Context, v Volume, name string, t *Set) (string, error) {
	s := d.s
	if !ValidName(name) {
		return "", &Error{Op: opCreate, Method: LVM, Volume: v.Device, Snapshot: name, Err: ErrName}
	}
	if !filepath.IsAbs(v.Device) || !fsTypeRe.MatchString(v.FSType) {
		return "", &Error{Op: opCreate, Method: LVM, Volume: v.Device, Err: ErrUnsupported,
			Cause: detail("file system %q on %q is not on a logical volume", v.FSType, v.Device)}
	}
	out, err := s.run(ctx, LVM, "find logical volume", v.Device, name,
		"lvs", "--noheadings", "--separator", "/", "-o", "vg_name,lv_name", v.Device)
	if err != nil {
		return "", err
	}
	lvs := parseLVs(out)
	if len(lvs) != 1 {
		return "", &Error{Op: "find logical volume", Method: LVM, Volume: v.Device, Err: ErrUnsupported,
			Cause: detail("lvs did not name one logical volume")}
	}
	vg, origin := lvs[0][0], lvs[0][1]
	snap := vg + "/" + name
	args := append([]string{"--snapshot", "--name", name}, s.cfg.lvmSizeArgs()...)
	if _, err := s.run(ctx, LVM, opCreate, v.Device, name, "lvcreate", append(args, vg+"/"+origin)...); err != nil {
		return "", err
	}
	s.logStep("source snapshot created", LVM, v.Device, name)
	t.push(func(ctx context.Context) error {
		if _, err := s.run(ctx, LVM, "remove", v.Device, name, "lvremove", "--force", snap); err != nil {
			return err
		}
		s.logStep("source snapshot removed", LVM, v.Device, name)
		return nil
	})
	mnt, err := s.mountPoint(LVM, v.Device, name, t)
	if err != nil {
		return "", err
	}
	opts := "ro"
	if v.FSType == "xfs" {
		// The snapshot carries the origin's UUID, which XFS refuses to
		// mount twice without nouuid.
		opts += ",nouuid"
	}
	if _, err := s.run(ctx, LVM, "mount", v.Device, name,
		"mount", "-t", v.FSType, "-o", opts, "/dev/"+snap, mnt); err != nil {
		return "", err
	}
	s.logStep("source snapshot mounted", LVM, v.Device, name)
	t.push(func(ctx context.Context) error { return s.unmount(ctx, LVM, v.Device, name, mnt) })
	return filepath.Join(mnt, v.Root), nil
}

// sweep removes leftover snapshot volumes; their mount points went first.
func (d *lvmDriver) sweep(ctx context.Context, _ []Volume) error {
	s := d.s
	out, err := s.run(ctx, LVM, "list logical volumes", "", "",
		"lvs", "--noheadings", "--separator", "/", "-o", "vg_name,lv_name")
	if err != nil {
		return err
	}
	var errs []error
	for _, lv := range parseLVs(out) {
		if !ValidName(lv[1]) {
			continue
		}
		snap := lv[0] + "/" + lv[1]
		if _, err := s.run(ctx, LVM, "remove leftover", lv[0], lv[1], "lvremove", "--force", snap); err != nil {
			errs = append(errs, err)
			continue
		}
		s.logStep("source snapshot leftover removed", LVM, lv[0], lv[1])
	}
	return errors.Join(errs...)
}

// parseLVs parses lvs output of "vg/lv" lines, keeping only valid names.
func parseLVs(out []byte) [][2]string {
	var lvs [][2]string
	for _, line := range strings.Split(string(out), "\n") {
		vg, lv, ok := strings.Cut(strings.TrimSpace(line), "/")
		if ok && lvmNameRe.MatchString(vg) && lvmNameRe.MatchString(lv) {
			lvs = append(lvs, [2]string{vg, lv})
		}
	}
	return lvs
}
