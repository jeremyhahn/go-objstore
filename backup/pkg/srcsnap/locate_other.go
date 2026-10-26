//go:build !linux && !darwin && !windows

package srcsnap

import "errors"

// platformLocator is a placeholder on systems with no snapshot method.
type platformLocator struct{}

var errNoLocator = errors.New("source snapshots are not supported on this system")

// Locate implements Locator.
func (platformLocator) Locate(string) (Volume, string, error) { return Volume{}, "", errNoLocator }

// MountPoints implements Locator.
func (platformLocator) MountPoints() (map[string]bool, error) { return nil, errNoLocator }
