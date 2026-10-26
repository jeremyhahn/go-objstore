package srcsnap

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
)

var (
	vssIDRe     = regexp.MustCompile(`^\{[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}\}$`)
	vssDeviceRe = regexp.MustCompile(`^\\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy[0-9]+$`)
	// vssVolumeRe is the volume paths the driver passes to PowerShell: a
	// drive root or a mounted folder, never a quote or a control
	// character, so the path can sit inside a single-quoted literal.
	vssVolumeRe = regexp.MustCompile(`^[A-Za-z]:\\([^'\x00-\x1f]*\\)?$`)
)

// vssDriver makes a Volume Shadow Copy through CIM, by running Windows
// PowerShell's Invoke-CimMethod on Win32_ShadowCopy.Create, reads it
// through its \\?\GLOBALROOT shadow device path, and removes the
// Win32_ShadowCopy instance on cleanup. Shadow copies are named by a GUID
// the system chooses, so the driver records it in a state file named with
// the prefix; Sweep deletes only shadow copies a state file records.
type vssDriver struct{ s *Snapshotter }

func (d *vssDriver) base(v Volume) string { return v.MountPoint }

// powershell runs script in Windows PowerShell with no profile and no
// prompt.
func (s *Snapshotter) powershell(ctx context.Context, op, volume, name, script string) (string, error) {
	out, err := s.run(ctx, VSS, op, volume, name, "powershell.exe",
		"-NoLogo", "-NoProfile", "-NonInteractive", "-Command", script)
	return strings.TrimSpace(string(out)), err
}

func vssCreateScript(volume string) string {
	return "$ErrorActionPreference = 'Stop'; " +
		"$r = Invoke-CimMethod -ClassName Win32_ShadowCopy -MethodName Create " +
		"-Arguments @{ Volume = '" + volume + "'; Context = 'ClientAccessible' }; " +
		"if ($r.ReturnValue -ne 0) { throw ('Win32_ShadowCopy.Create returned ' + $r.ReturnValue) }; " +
		"$r.ShadowID"
}

func vssDeviceScript(id string) string {
	return "$ErrorActionPreference = 'Stop'; " +
		"(Get-CimInstance -ClassName Win32_ShadowCopy -Filter \"ID='" + id + "'\").DeviceObject"
}

func vssDeleteScript(id string) string {
	return "$ErrorActionPreference = 'Stop'; " +
		"Get-CimInstance -ClassName Win32_ShadowCopy -Filter \"ID='" + id + "'\" | Remove-CimInstance"
}

func (d *vssDriver) create(ctx context.Context, v Volume, name string, t *Set) (string, error) {
	s := d.s
	vol := v.MountPoint
	if !ValidName(name) {
		return "", &Error{Op: opCreate, Method: VSS, Volume: vol, Snapshot: name, Err: ErrName}
	}
	if !vssVolumeRe.MatchString(vol) {
		return "", &Error{Op: opCreate, Method: VSS, Volume: vol, Err: ErrUnsupported,
			Cause: detail("%q is not a volume path", vol)}
	}
	id, err := s.powershell(ctx, opCreate, vol, name, vssCreateScript(vol))
	if err != nil {
		return "", err
	}
	if !vssIDRe.MatchString(id) {
		return "", &Error{Op: opCreate, Method: VSS, Volume: vol, Snapshot: name, Err: ErrCommand,
			Cause: detail("PowerShell printed no shadow copy ID")}
	}
	s.logStep("source snapshot created", VSS, vol, name)
	// The state file goes only once the shadow copy is gone, so a failed
	// removal leaves a record for the next sweep.
	werr := s.writeState(VSS, vol, name, id)
	t.push(func(ctx context.Context) error {
		if err := s.deleteVSS(ctx, vol, name, id); err != nil {
			return err
		}
		if werr != nil {
			return nil
		}
		return s.removeState(VSS, name)
	})
	if werr != nil {
		return "", werr
	}
	dev, err := s.powershell(ctx, "find shadow device", vol, name, vssDeviceScript(id))
	if err != nil {
		return "", err
	}
	if !vssDeviceRe.MatchString(dev) {
		return "", &Error{Op: "find shadow device", Method: VSS, Volume: vol, Snapshot: name, Err: ErrCommand,
			Cause: detail("PowerShell printed no shadow copy device")}
	}
	s.logStep("source snapshot mounted", VSS, vol, name)
	return dev + `\`, nil
}

// deleteVSS deletes the shadow copy id.
func (s *Snapshotter) deleteVSS(ctx context.Context, vol, name, id string) error {
	if _, err := s.powershell(ctx, "remove", vol, name, vssDeleteScript(id)); err != nil {
		return err
	}
	s.logStep("source snapshot removed", VSS, vol, name)
	return nil
}

// sweep deletes the shadow copies that leftover state files record, then
// the state files.
func (d *vssDriver) sweep(ctx context.Context, _ []Volume) error {
	s := d.s
	states, err := s.states(VSS)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(states))
	for name := range states {
		names = append(names, name)
	}
	sort.Strings(names)
	var errs []error
	for _, name := range names {
		id := states[name]
		if !vssIDRe.MatchString(id) {
			errs = append(errs, &Error{Op: opSweep, Method: VSS, Snapshot: name, Err: ErrName,
				Cause: detail("the state file holds no shadow copy ID")})
			continue
		}
		if _, err := s.powershell(ctx, "remove leftover", "", name, vssDeleteScript(id)); err != nil {
			errs = append(errs, err)
			continue
		}
		s.logStep("source snapshot leftover removed", VSS, "", name)
		errs = append(errs, s.removeState(VSS, name))
	}
	return errors.Join(errs...)
}
